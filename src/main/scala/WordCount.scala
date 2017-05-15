import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
/**
  * Created by Avik on 5/11/2017.
  */
object WordCount {
 def main(ags: Array[String]): Unit = {

   val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local").set("spark.executor.memory", "1g")
   val sc = new SparkContext(conf)
   val input = sc.textFile("file:///D://text.txt")
   val words = input.flatMap(line => line.split(" "))
   val counts = words.map(word => (word,1)).reduceByKey{case(x,y) => x + y }
   counts.saveAsTextFile("file:///E://avik.txt")
 }
}
