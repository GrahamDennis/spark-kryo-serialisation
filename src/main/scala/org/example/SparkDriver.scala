/* SparkDriver.scala */

package org.example

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.serializer.KryoRegistrator

import scala.util.Random

class MyCustomClass

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    Console.err.println("################# MyRegistrator called")
    kryo.register(classOf[MyCustomClass])
  }
}

object SparkDriver {
  def main(args: Array[String]) {
    val conf = new SparkConf()
               .setAll(Map( "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
                            "spark.kryo.registrator" -> "org.example.MyRegistrator",
                            "spark.task.maxFailures" -> "1"
                          ))

    val sc = new SparkContext(conf.setAppName("Spark with Kryo Serialisation"))

    val numElements = 1000

    // We cache this RDD to make sure the values are actually transported and not recomputed.
    val cachedRDD = sc.parallelize((0 until numElements).map((_, new MyCustomClass)), 10)
                    .cache()

    // Randomly mix the keys so that the join below will require a shuffle with each partition sending data to
    // many other partitions.
    val randomisedRDD = cachedRDD.map({ case (index, customObject) => ((new Random).nextInt(), customObject)})

    // Join the two RDDs, and force evaluation.
    val localResults = randomisedRDD.join(cachedRDD).collect()
  }

}