import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object WordFrequency {
  def main(args: Array[String]):Unit = {

    // Suppress unessesary log output
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    var conf = new SparkConf()
    conf.setAppName("WordFrequency")
    conf.setMaster("local[2]")
    var sc = new SparkContext(conf)


    var productFile = sc.textFile("data/products.csv")

    // Split file into words bunch
    // Then we will get flattened space
    var words = productFile.flatMap(x => x.split(" "))

    // Add an number for each word. Then it produces tuples like ('mom',1)
    var wordsNumbers = words.map(x=> (x,1))

    // Apply reduceByKey. Then for each 'mom' key it apply reduce function.
    // That var will be an accumulator which stores count of  works with specific keys
    var wordCounters = wordsNumbers.reduceByKey((a,b) => a+b)

    // Word amount
    var fullCount = words.count().toFloat
    // Calculate frequency

    var wordFrq = wordCounters.map(x=>(x._1,x._2,x._2.toFloat / fullCount))

    wordFrq.foreach(println)


  }
}
