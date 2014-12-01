package edu.berkeley.cs.amplab.mlmatrix.util

import edu.berkeley.cs.amplab.mlmatrix._

import org.scalatest.FunSuite
import org.apache.spark.SparkContext

class UtilsSuite extends FunSuite with LocalSparkContext {

  test("multiple RDD coalesce") {
    sc = new SparkContext("local", "test")
    val rdd1 = sc.parallelize(1 to 10, 5)
    val rdd2 = rdd1.map(x => x + 10.0)

    val coalescer = Utils.createCoalescer(rdd1, 2)
    val coalesced1 = coalescer(rdd1)
    val coalesced2 = coalescer(rdd2)

    val diffs = coalesced1.zip(coalesced2).map(x => x._2 - x._1).collect()
    assert(diffs.forall(x => x == 10.0))
  }
}
