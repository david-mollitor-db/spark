/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Max, Min}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, Filter, Join, LeafNode, LogicalPlan, Project, Subquery, Union}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{JOIN, PYTHON_UDF}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AtomicType, DataType, DateType, NumericType, TimestampType}

/**
 * For an equi-join that also carries a one-sided inequality between a column on each side
 * (e.g. {{{inventory.qoh < catalog_sales.quantity}}}), derive the necessary min/max bound
 * implied by that inequality and inject it as a filter on the constrained side
 * (e.g. {{{inventory.qoh < MAX(catalog_sales.quantity)}}}), pruning rows before the join has to
 * buffer them. The bound is computed at runtime via a scalar subquery over the other side, so the
 * result is unchanged. This is modeled on [[InjectRuntimeFilter]], but keyed on the inequality
 * condition rather than the equi-join keys.
 *
 * Soundness: for a joined pair to satisfy `x < y`, `x` must be `< MAX(y)`; likewise `x > y` implies
 * `x > MIN(y)`. So the injected predicate is a necessary condition and removes no result rows.
 *
 * Both operands of the inequality must be columns. The filter costs an extra aggregate, so, as in
 * [[InjectRuntimeFilter]], it is only injected when it is likely to pay off: the side it is
 * applied to must have a large scan, the bound must be cheap to compute (see [[boundSource]]),
 * column statistics (when available) must not show the bound to be unselective, and the side must
 * not already carry the filter. Statistics only gate the injection; the filter always uses the
 * bound computed at runtime.
 */
object InjectDynamicMinMaxFilter extends Rule[LogicalPlan]
  with PredicateHelper with JoinSelectionHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case s: Subquery if s.correlated => plan
    case _ if !conf.getConf(SQLConf.DYNAMIC_MINMAX_FILTER_ENABLED) => plan
    case _ => tryInjectMinMaxFilter(plan)
  }

  private def tryInjectMinMaxFilter(plan: LogicalPlan): LogicalPlan =
    plan.transformUpWithPruning(_.containsPattern(JOIN)) {
      case join @ Join(left, right, joinType, Some(condition), _)
          if hasEquiJoinKey(condition, left, right) =>
        var newLeft = left
        var newRight = right
        splitConjunctivePredicates(condition).foreach {
          case c: BinaryComparison if isRangeComparison(c) =>
            normalize(c, left, right).foreach { case (leftOp, rightOp, op) =>
              // Skip if either side already carries the filter derived from this inequality,
              // e.g. from an earlier run of this rule.
              val alreadyFiltered =
                hasMinMaxFilter(newRight, rightOp, boundColumns(leftOp, newLeft)) ||
                  hasMinMaxFilter(newLeft, leftOp, boundColumns(rightOp, newRight))
              // Prefer pruning the (large) right side; fall back to the left side.
              if (!alreadyFiltered && (newRight eq right) && canPruneRight(joinType)) {
                injectRight(op, leftOp, rightOp, newLeft, newRight).foreach(newRight = _)
              }
              if (!alreadyFiltered && (newLeft eq left) && (newRight eq right) &&
                  canPruneLeft(joinType)) {
                injectLeft(op, leftOp, rightOp, newLeft, newRight).foreach(newLeft = _)
              }
            }
          case _ =>
        }
        if ((newLeft ne left) || (newRight ne right)) {
          join.copy(left = newLeft, right = newRight)
        } else {
          join
        }
    }

  private sealed trait Op
  private case object Lt extends Op
  private case object Lte extends Op
  private case object Gt extends Op
  private case object Gte extends Op

  private def isRangeComparison(c: BinaryComparison): Boolean = c match {
    case _: LessThan | _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual => true
    case _ => false
  }

  private def hasEquiJoinKey(
      condition: Expression, left: LogicalPlan, right: LogicalPlan): Boolean =
    splitConjunctivePredicates(condition).exists {
      case EqualTo(l, r) =>
        (canEvaluate(l, left) && canEvaluate(r, right)) ||
          (canEvaluate(l, right) && canEvaluate(r, left))
      case _ => false
    }

  /**
   * Normalize a range comparison to `(leftOperand op rightOperand)` where `leftOperand` is a column
   * of the join's left child and `rightOperand` of the right child. Returns None unless both
   * operands are orderable columns, one from each side. Computed operands are not supported:
   * column statistics cannot estimate them, so the filter could not be gated on selectivity.
   */
  private def normalize(
      c: BinaryComparison,
      left: LogicalPlan,
      right: LogicalPlan): Option[(Attribute, Attribute, Op)] = (c.left, c.right) match {
    case (a: Attribute, b: Attribute) if a.dataType.isInstanceOf[AtomicType] =>
      val op = c match {
        case _: LessThan => Lt
        case _: LessThanOrEqual => Lte
        case _: GreaterThan => Gt
        case _: GreaterThanOrEqual => Gte
      }
      if (left.outputSet.contains(a) && right.outputSet.contains(b)) {
        Some((a, b, op))
      } else if (right.outputSet.contains(a) && left.outputSet.contains(b)) {
        Some((b, a, flip(op)))
      } else {
        None
      }
    case _ => None
  }

  private def flip(op: Op): Op = op match {
    case Lt => Gt
    case Lte => Gte
    case Gt => Lt
    case Gte => Lte
  }

  // Prune the right child. For `leftOp op rightOp` the necessary bound on rightOp is:
  //   leftOp <  rightOp -> rightOp >  MIN(leftOp);  leftOp <= rightOp -> rightOp >= MIN(leftOp)
  //   leftOp >  rightOp -> rightOp <  MAX(leftOp);  leftOp >= rightOp -> rightOp <= MAX(leftOp)
  private def injectRight(
      op: Op, leftOp: Attribute, rightOp: Attribute,
      leftPlan: LogicalPlan, rightPlan: LogicalPlan): Option[LogicalPlan] = op match {
    case Lt => build(rightOp, leftOp, leftPlan, upperBound = false, GreaterThan(_, _), rightPlan)
    case Lte =>
      build(rightOp, leftOp, leftPlan, upperBound = false, GreaterThanOrEqual(_, _), rightPlan)
    case Gt => build(rightOp, leftOp, leftPlan, upperBound = true, LessThan(_, _), rightPlan)
    case Gte =>
      build(rightOp, leftOp, leftPlan, upperBound = true, LessThanOrEqual(_, _), rightPlan)
  }

  // Prune the left child. For `leftOp op rightOp` the necessary bound on leftOp is:
  //   leftOp <  rightOp -> leftOp <  MAX(rightOp);  leftOp <= rightOp -> leftOp <= MAX(rightOp)
  //   leftOp >  rightOp -> leftOp >  MIN(rightOp);  leftOp >= rightOp -> leftOp >= MIN(rightOp)
  private def injectLeft(
      op: Op, leftOp: Attribute, rightOp: Attribute,
      leftPlan: LogicalPlan, rightPlan: LogicalPlan): Option[LogicalPlan] = op match {
    case Lt => build(leftOp, rightOp, rightPlan, upperBound = true, LessThan(_, _), leftPlan)
    case Lte =>
      build(leftOp, rightOp, rightPlan, upperBound = true, LessThanOrEqual(_, _), leftPlan)
    case Gt => build(leftOp, rightOp, rightPlan, upperBound = false, GreaterThan(_, _), leftPlan)
    case Gte =>
      build(leftOp, rightOp, rightPlan, upperBound = false, GreaterThanOrEqual(_, _), leftPlan)
  }

  /**
   * Builds `Filter(mkPred(targetCol, ScalarSubquery(agg(boundCol) over buildPlan)), constrainPlan)`
   * where `agg` is MAX for an upper bound and MIN for a lower bound, gated so it is only worth the
   * extra aggregate. The aggregate may instead run over the scan `boundCol` originates from, see
   * [[boundSource]].
   */
  private def build(
      targetCol: Attribute,
      boundCol: Attribute,
      buildPlan: LogicalPlan,
      upperBound: Boolean,
      mkPred: (Expression, Expression) => Predicate,
      constrainPlan: LogicalPlan): Option[LogicalPlan] = {
    if (buildPlan.isStreaming || !satisfyByteSizeRequirement(constrainPlan) ||
        !isEstimatedSelective(targetCol, constrainPlan, boundCol, buildPlan, upperBound)) {
      return None
    }
    boundSource(boundCol, buildPlan, constrainPlan).flatMap { case (col, source) =>
      val agg = if (upperBound) Max(col) else Min(col)
      val alias = Alias(agg.toAggregateExpression(), "bound")()
      val aggregate = ConstantFolding(ColumnPruning(Aggregate(Nil, Seq(alias), source)))
      if (aggregate.containsPattern(PYTHON_UDF)) {
        None
      } else {
        Some(Filter(mkPred(targetCol, ScalarSubquery(aggregate, Nil)), constrainPlan))
      }
    }
  }

  /**
   * The plan to compute the bound over, and the column of it to aggregate:
   *  - `buildPlan` itself, if it is small enough; this gives the tightest bound.
   *  - Otherwise the scan `boundCol` originates from, if reading that column is estimated to cost
   *    no more than the largest scan the filter prunes. The scan's values of the column are a
   *    superset of `buildPlan`'s (see [[trackToScan]]), so its MIN/MAX is a looser but still
   *    necessary bound, and computing it reads one column of one scan rather than re-evaluating
   *    `buildPlan`, which may contain joins.
   */
  private def boundSource(
      boundCol: Attribute,
      buildPlan: LogicalPlan,
      constrainPlan: LogicalPlan): Option[(Attribute, LogicalPlan)] = {
    if (buildPlan.stats.sizeInBytes <= conf.runtimeFilterCreationSideThreshold) {
      Some((boundCol, buildPlan))
    } else {
      trackToScan(boundCol, buildPlan).filter { case (col, scan) =>
        columnScanBytes(col, scan) <= InjectRuntimeFilter.maxScanByteSize(constrainPlan)
      }
    }
  }

  /**
   * Tracks `attr` down `plan` to the leaf it is read from, through operators that pass the column
   * through unchanged. Those can only drop or duplicate rows, or pad the column with NULLs (which
   * MIN and MAX ignore), so the leaf's values of the column are a superset of `plan`'s. Returns
   * None at a Union or Expand, whose output column combines several inputs, and where the column
   * is computed: evaluating an expression over rows `plan` filters out could change the bound's
   * semantics or raise errors the query would not.
   */
  private def trackToScan(attr: Attribute, plan: LogicalPlan): Option[(Attribute, LogicalPlan)] =
    plan match {
      case leaf: LeafNode => if (leaf.outputSet.contains(attr)) Some((attr, leaf)) else None
      case p: Project => trackThroughAlias(attr, getAliasMap(p), p.child)
      case a: Aggregate => trackThroughAlias(attr, getAliasMap(a), a.child)
      case _: Union | _: Expand => None
      case other => other.children.find(_.outputSet.contains(attr)).flatMap(trackToScan(attr, _))
    }

  private def trackThroughAlias(
      attr: Attribute,
      aliases: AttributeMap[Alias],
      child: LogicalPlan): Option[(Attribute, LogicalPlan)] = aliases.get(attr) match {
    case Some(Alias(source: Attribute, _)) => trackToScan(source, child)
    case Some(_) => None
    case None => trackToScan(attr, child)
  }

  // Estimated bytes read to scan only `col` of `scan`: its size prorated by the column's share of
  // the row width, as columnar sources read just the requested column.
  private def columnScanBytes(col: Attribute, scan: LogicalPlan): BigInt = {
    val stats = scan.stats
    stats.sizeInBytes * EstimationUtils.getSizePerRow(Seq(col), stats.attributeStats) /
      EstimationUtils.getSizePerRow(scan.output, stats.attributeStats)
  }

  // The columns a bound derived from `col` in `plan` may aggregate: `col`, or the scan column it
  // originates from.
  private def boundColumns(col: Attribute, plan: LogicalPlan): Seq[Attribute] =
    col +: trackToScan(col, plan).map(_._1).toSeq

  // Returns true if the largest scan under `filterApplicationSide` is large enough to be worth
  // pruning. As in InjectRuntimeFilter, a scan of unknown size counts as 0.
  private def satisfyByteSizeRequirement(filterApplicationSide: LogicalPlan): Boolean =
    InjectRuntimeFilter.maxScanByteSize(filterApplicationSide) >=
      conf.getConf(SQLConf.DYNAMIC_MINMAX_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD)

  /**
   * Estimates, from column min/max statistics and assuming a uniform distribution, the fraction of
   * `targetCol` values the bound prunes, and returns whether it reaches
   * `minEstimatedPruneRatio`. Returns true when either column lacks usable statistics.
   */
  private def isEstimatedSelective(
      targetCol: Expression,
      applicationPlan: LogicalPlan,
      boundCol: Expression,
      creationPlan: LogicalPlan,
      upperBound: Boolean): Boolean = {
    val prunedRatio = for {
      (targetMin, targetMax) <- columnRange(targetCol, applicationPlan)
      if targetMax > targetMin
      (boundMin, boundMax) <- columnRange(boundCol, creationPlan)
    } yield {
      val pruned = if (upperBound) targetMax - boundMax else boundMin - targetMin
      math.min(math.max(pruned / (targetMax - targetMin), 0.0), 1.0)
    }
    prunedRatio.forall(
      _ >= conf.getConf(SQLConf.DYNAMIC_MINMAX_FILTER_MIN_ESTIMATED_PRUNE_RATIO))
  }

  // The (min, max) of a column from plan statistics, falling back to the statistics of the scan
  // the column originates from. None if unavailable or the type has no numeric order.
  private def columnRange(col: Expression, plan: LogicalPlan): Option[(Double, Double)] = {
    def range(e: Expression, p: LogicalPlan): Option[(Double, Double)] = e match {
      case a: Attribute if hasNumericRange(a.dataType) =>
        p.stats.attributeStats.get(a).flatMap { stat =>
          for (min <- stat.min; max <- stat.max) yield {
            (EstimationUtils.toDouble(min, a.dataType), EstimationUtils.toDouble(max, a.dataType))
          }
        }
      case _ => None
    }
    range(col, plan).orElse {
      findExpressionAndTrackLineageDown(col, plan).flatMap { case (e, origin) => range(e, origin) }
    }
  }

  private def hasNumericRange(dataType: DataType): Boolean = dataType match {
    case _: NumericType | DateType | TimestampType => true
    case _ => false
  }

  // Whether `plan` already filters `targetCol` against a scalar subquery computing MIN or MAX of
  // one of `boundCols`, i.e. the filter this rule derives from a `targetCol op boundCol`
  // inequality.
  private def hasMinMaxFilter(
      plan: LogicalPlan, targetCol: Attribute, boundCols: Seq[Attribute]): Boolean = plan.exists {
    case Filter(condition, _) =>
      splitConjunctivePredicates(condition).exists {
        case c: BinaryComparison if isRangeComparison(c) =>
          c.left.semanticEquals(targetCol) && isMinMaxBound(c.right, boundCols)
        case _ => false
      }
    case _ => false
  }

  private def isMinMaxBound(e: Expression, boundCols: Seq[Attribute]): Boolean = e match {
    case s: ScalarSubquery => s.plan match {
      case a: Aggregate if a.groupingExpressions.isEmpty =>
        a.aggregateExpressions.exists(_.exists {
          case Max(child) => boundCols.exists(_.semanticEquals(child))
          case Min(child) => boundCols.exists(_.semanticEquals(child))
          case _ => false
        })
      case _ => false
    }
    case _ => false
  }
}
