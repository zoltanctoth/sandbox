// spark-shell --num-executors 1 --driver-memory=14g --executor-cores=1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.util.Random

// val conf = new SparkConf().setAppName("test-partitions")
// conf.setMaster("local[1]")
// val sc = new SparkContext(conf)

val s = (1 to 100) map { _ => Random.nextInt() }
val rdd: RDD[Int] = sc.parallelize(s)

sc.defaultParallelism

val pairRdd = rdd map { ("key_" + Random.nextInt(10).toString(), _) }
pairRdd.groupByKey(100).mapPartitionsWithIndex(
  (i: Int, x: Iterator[(String, Iterable[Int])]) => x.map((x: (String, Iterable[Int]))
    => (i + " " + x._1, x._2.size)), false).collect()

