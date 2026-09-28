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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.ScalarSubquery
import org.apache.spark.sql.catalyst.expressions.aggregate.{Max, Min}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class InjectDynamicMinMaxFilterSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  // The test tables are tiny, so lift the application-side scan size gate.
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.DYNAMIC_MINMAX_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key, "0")

  // catalog_sales-like build side (small quantities) joined to an inventory-like probe side
  // (larger on-hand values) with a one-sided inequality, mirroring TPC-DS q72.
  private val query =
    "SELECT cs.k, cs.qty, inv.qoh FROM cs JOIN inv ON cs.k = inv.k WHERE inv.qoh < cs.qty"

  private def withData(f: => Unit): Unit = withTable("cs", "inv") {
    spark.range(0, 100).selectExpr("id % 10 AS k", "CAST(id % 5 AS INT) AS qty")
      .write.saveAsTable("cs")
    spark.range(0, 1000).selectExpr("id % 10 AS k", "CAST(id % 50 AS INT) AS qoh")
      .write.saveAsTable("inv")
    f
  }

  /** True if `plan` has a Filter whose predicate references a scalar subquery over MIN/MAX. */
  private def hasMinMaxFilter(plan: LogicalPlan): Boolean = plan.exists {
    case f: Filter =>
      f.condition.exists {
        case s: ScalarSubquery =>
          s.plan.exists(_.expressions.exists(_.exists {
            case _: Max | _: Min => true
            case _ => false
          }))
        case _ => false
      }
    case _ => false
  }

  private def collectWith(enabled: Boolean): Seq[Row] =
    withSQLConf(SQLConf.DYNAMIC_MINMAX_FILTER_ENABLED.key -> enabled.toString) {
      sql(query).collect().toSeq
    }

  test("results are identical with the min/max filter on and off") {
    withData {
      val expected = collectWith(enabled = false)
      withSQLConf(SQLConf.DYNAMIC_MINMAX_FILTER_ENABLED.key -> "true") {
        checkAnswer(sql(query), expected)
      }
    }
  }

  test("the min/max filter is injected") {
    withData {
      withSQLConf(SQLConf.DYNAMIC_MINMAX_FILTER_ENABLED.key -> "true") {
        val plan = sql(query).queryExecution.optimizedPlan
        assert(hasMinMaxFilter(plan), s"expected an injected min/max filter, but got:\n$plan")
      }
      withSQLConf(SQLConf.DYNAMIC_MINMAX_FILTER_ENABLED.key -> "false") {
        val plan = sql(query).queryExecution.optimizedPlan
        assert(!hasMinMaxFilter(plan), s"expected no min/max filter when disabled, but got:\n$plan")
      }
    }
  }

  test("a bound computed over the originating scan gives identical results") {
    withData {
      // The filter on `cs` makes the scan's MAX(qty) looser than the build side's; the creation
      // side threshold of 0 forces the bound onto the scan.
      val filtered = "SELECT cs.k, cs.qty, inv.qoh FROM (SELECT * FROM cs WHERE qty < 3) cs " +
        "JOIN inv ON cs.k = inv.k WHERE inv.qoh < cs.qty"
      val expected = withSQLConf(SQLConf.DYNAMIC_MINMAX_FILTER_ENABLED.key -> "false") {
        sql(filtered).collect().toSeq
      }
      withSQLConf(
          SQLConf.DYNAMIC_MINMAX_FILTER_ENABLED.key -> "true",
          SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "0") {
        val df = sql(filtered)
        val plan = df.queryExecution.optimizedPlan
        assert(hasMinMaxFilter(plan), s"expected an injected min/max filter, but got:\n$plan")
        checkAnswer(df, expected)
      }
    }
  }

  test("NULL operands stay consistent with the join") {
    withTable("cs", "inv") {
      Seq((1, Some(3)), (1, None), (2, Some(4))).toDF("k", "qty").write.saveAsTable("cs")
      Seq((1, Some(2)), (1, None), (2, Some(9))).toDF("k", "qoh").write.saveAsTable("inv")
      val expected = collectWith(enabled = false)
      withSQLConf(SQLConf.DYNAMIC_MINMAX_FILTER_ENABLED.key -> "true") {
        checkAnswer(sql(query), expected)
      }
    }
  }

  test("empty build side prunes the probe side to empty, matching the join") {
    withTable("cs", "inv") {
      spark.range(0).selectExpr("CAST(id AS BIGINT) AS k", "CAST(id AS INT) AS qty")
        .write.saveAsTable("cs")
      Seq((1, 2), (1, 5), (2, 9)).toDF("k", "qoh").write.saveAsTable("inv")
      val expected = collectWith(enabled = false)
      assert(expected.isEmpty)
      withSQLConf(SQLConf.DYNAMIC_MINMAX_FILTER_ENABLED.key -> "true") {
        checkAnswer(sql(query), expected)
      }
    }
  }
}
