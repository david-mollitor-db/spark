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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, BinaryComparison, Expression, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Max, Min}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Filter, Join, LocalRelation, LogicalPlan, Union}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
import org.apache.spark.sql.internal.SQLConf

class InjectDynamicMinMaxFilterSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("InjectDynamicMinMaxFilter", FixedPoint(1),
      InjectDynamicMinMaxFilter) :: Nil
  }

  private val a = $"a".int
  private val b = $"b".int
  private val c = $"c".int
  private val d = $"d".int
  private val e = $"e".int

  // Prunes the right side with `d < MAX(b)`, the bound computed over the left side.
  private val condition = (a === c) && (d < b)

  private val t1 = LocalRelation(a, b)
  private val t2 = LocalRelation(c, d)

  private val MB = 1024L * 1024L

  private def relation(size: BigInt, output: AttributeReference*)(
      stats: (Attribute, ColumnStat)*): StatsTestPlan =
    StatsTestPlan(output, rowCount = 1000, AttributeMap(stats), size = Some(size))

  private def withMinMaxFilter(confs: (String, String)*)(f: => Unit): Unit =
    withSQLConf((SQLConf.DYNAMIC_MINMAX_FILTER_ENABLED.key -> "true") +: confs: _*)(f)

  // Lifts the scan-size gate, for plans over LocalRelations whose scans have no size.
  private val noScanSizeThreshold =
    SQLConf.DYNAMIC_MINMAX_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0"

  /** The comparisons in `plan` whose right side is a scalar subquery over MIN/MAX. */
  private def minMaxFilters(plan: LogicalPlan): Seq[BinaryComparison] = plan.flatMap {
    case f: Filter =>
      splitConjunctivePredicates(f.condition).collect {
        case cmp: BinaryComparison if cmp.right.isInstanceOf[ScalarSubquery] &&
            cmp.right.asInstanceOf[ScalarSubquery].plan.expressions.exists(_.exists {
              case _: Max | _: Min => true
              case _ => false
            }) => cmp
      }
    case _ => Nil
  }

  private def assertInjectedOn(plan: LogicalPlan, target: Attribute): Unit = {
    val filters = minMaxFilters(plan)
    assert(filters.size == 1, s"expected one injected min/max filter, but got:\n$plan")
    assert(filters.head.left.semanticEquals(target), s"expected a filter on $target:\n$plan")
  }

  test("inject a min/max filter for an equi-join with a one-sided inequality") {
    withMinMaxFilter(noScanSizeThreshold) {
      val query = t1.join(t2, Inner, Some(condition)).analyze
      assertInjectedOn(Optimize.execute(query), d)
    }
  }

  test("no injection when the feature is disabled") {
    withSQLConf(SQLConf.DYNAMIC_MINMAX_FILTER_ENABLED.key -> "false", noScanSizeThreshold) {
      val query = t1.join(t2, Inner, Some(condition)).analyze
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("no injection without an equi-join key") {
    withMinMaxFilter(noScanSizeThreshold) {
      val query = t1.join(t2, Inner, Some(d < b)).analyze
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("no injection without a range comparison across the two sides") {
    withMinMaxFilter(noScanSizeThreshold) {
      val query = t1.join(t2, Inner, Some((a === c) && (b > 5))).analyze
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("no injection when the application-side scan is below the size threshold") {
    withMinMaxFilter() {
      val query = t1.join(t2, Inner, Some(condition)).analyze
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("inject when the application-side scan is large and the creation side is small") {
    withMinMaxFilter() {
      val query = relation(MB, a, b)().join(relation(1024 * MB, c, d)(), Inner, Some(condition))
      assertInjectedOn(Optimize.execute(query), d)
    }
  }

  test("no injection when an inequality operand is computed") {
    withMinMaxFilter(noScanSizeThreshold) {
      val query = t1.join(t2, Inner, Some((a === c) && (d < b + 1))).analyze
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("compute the bound over the originating scan when the creation side is large") {
    withMinMaxFilter() {
      val scan = relation(512 * MB, a, b)()
      val creationSide = scan.join(relation(MB, e)(), Inner, Some(a === e))
      val query = creationSide.join(relation(1024 * MB, c, d)(), Inner, Some(condition))
      val optimized = Optimize.execute(query)
      assertInjectedOn(optimized, d)
      val boundPlan = minMaxFilters(optimized).head.right.asInstanceOf[ScalarSubquery].plan
      assert(!boundPlan.exists(_.isInstanceOf[Join]), s"expected no join in:\n$boundPlan")
      assert(boundPlan.exists(_ eq scan), s"expected the bound over the scan:\n$boundPlan")
    }
  }

  test("no injection when reading the bound's scan costs more than the scan it prunes") {
    withMinMaxFilter() {
      // LeftOuter so that only the right side may be pruned.
      val query = relation(4096 * MB, a, b)()
        .join(relation(1024 * MB, c, d)(), LeftOuter, Some(condition))
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("track the bound through a column alias but not a computed column") {
    withMinMaxFilter() {
      val x = $"x".int
      def query(bExpr: Expression): LogicalPlan = {
        val alias = Alias(bExpr, "b")()
        val creationSide = relation(512 * MB, a, x)().select(a, alias)
        creationSide.join(relation(1024 * MB, c, d)(), LeftOuter,
          Some((a === c) && (d < alias.toAttribute)))
      }
      assertInjectedOn(Optimize.execute(query(x)), d)
      val computed = query(x + 1)
      comparePlans(Optimize.execute(computed), computed)
    }
  }

  test("no bound over a single child of a union") {
    withMinMaxFilter() {
      // The union's `b` also takes values from its second child, so a bound computed over the
      // first child's scan alone could drop matching rows.
      val union = Union(relation(512 * MB, a, b)(), relation(512 * MB, $"a2".int, $"b2".int)())
      val query = union.join(relation(1024 * MB, c, d)(), LeftOuter, Some(condition))
      comparePlans(Optimize.execute(query), query)
    }
  }

  test("column statistics gate the injection on its estimated selectivity") {
    withMinMaxFilter() {
      val dStat = d -> ColumnStat(min = Some(0), max = Some(1000))
      def query(bMax: Int): LogicalPlan =
        relation(MB, a, b)(b -> ColumnStat(min = Some(0), max = Some(bMax)))
          .join(relation(1024 * MB, c, d)(dStat), Inner, Some(condition))
      // `d < MAX(b)` prunes an estimated 90% of `d` in [0, 1000].
      assertInjectedOn(Optimize.execute(query(bMax = 100)), d)
      // An estimated 5% is below the default minimum prune ratio.
      comparePlans(Optimize.execute(query(bMax = 950)), query(bMax = 950))
      // The bound is above every `d`, so nothing is pruned.
      comparePlans(Optimize.execute(query(bMax = 2000)), query(bMax = 2000))
      withSQLConf(SQLConf.DYNAMIC_MINMAX_FILTER_MIN_ESTIMATED_PRUNE_RATIO.key -> "0.01") {
        assertInjectedOn(Optimize.execute(query(bMax = 950)), d)
      }
    }
  }

  test("inject when column statistics are missing on either side") {
    withMinMaxFilter() {
      val dStat = d -> ColumnStat(min = Some(0), max = Some(1000))
      val query = relation(MB, a, b)()
        .join(relation(1024 * MB, c, d)(dStat), Inner, Some(condition))
      assertInjectedOn(Optimize.execute(query), d)
    }
  }

  test("no re-injection on a join that already carries the filter") {
    withMinMaxFilter(noScanSizeThreshold) {
      val once = Optimize.execute(t1.join(t2, Inner, Some(condition)).analyze)
      val twice = Optimize.execute(once)
      assertInjectedOn(twice, d)
      assert(twice.fastEquals(once))
    }
  }
}
