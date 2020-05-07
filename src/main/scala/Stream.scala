import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.log4j._
object Stream extends App {
 val conf=new SparkConf()
   .setMaster("local[12]")
   .setAppName("stream")

  val streamingContext=new StreamingContext(conf,Seconds.apply(5))
  val socketStream=streamingContext.socketTextStream("localhost",9999)

  val lines=socketStream.flatMap(lines=>lines.split(" "))
  val words=lines.map(x=>(x,1))
  val count=words.reduceByKey((x,y)=>x+y)
  count.print

 streamingContext.start()

 val logger=Logger.getRootLogger         //these two lines of code is for not getting too much logs on console
 val level=logger.setLevel(Level.ERROR)

 streamingContext.awaitTermination


}
