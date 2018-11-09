import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Superheroes {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
    conf.setAppName("SuperHeroes")
    conf.setMaster("local[2]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    val inputFile = "superheroes.csv"

    // Load our input data.
    val input = sc.textFile(inputFile)
    // Split up into words.

    val split = input.map(s => s.split(";"))

    var countHero = split.map(s => (s(1), 1)).reduceByKey((a, b) => a + b)
    // Transform into word and count.
    val countOfKilled = split.map(s => (s(1), s(2).toInt)).reduceByKey(_ + _)

    println("Count of Heroes")
    countHero.foreach(println)
    println("Count Of Killed Enemies")
    countOfKilled.foreach(println)
  }
}
