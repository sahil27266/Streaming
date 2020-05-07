import org.apache.spark.SparkConf
import org.apache.log4j._
import org.apache.spark.streaming.{Seconds, StreamingContext}
object WindowStreamWorldCount extends App {
val conf=new SparkConf().
  setAppName("window")
  .setMaster("local[12]")

   val checkpointdir="  Enter the checkpointing location"

  val ssc=new StreamingContext(conf,Seconds.apply(10))
  ssc.checkpoint(checkpointdir)
  val rootLogger=Logger.getRootLogger
  val level=rootLogger.setLevel(Level.ERROR)
  val inputStream=ssc.socketTextStream("localhost",7778)
  val wordStream=inputStream.flatMap(_.split(" "))
  val pairs=wordStream.map(x=>(x,1))
  val countStream=pairs.reduceByKeyAndWindow(_+_,_-_,Seconds.apply(40),Seconds.apply(20),2)
  countStream.print


  ssc.start()

  ssc.awaitTermination

}
