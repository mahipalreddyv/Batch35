
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object ReadJson2 {
  def main(args : Array[String]): Unit = {  
   
    var schmapath=args(0)
    var datapath=args(1)
    var outputpath=args(2)
    val spark=SparkSession.builder().master("local[*]").appName("Readjson").getOrCreate()
   
    var structSchema=spark.read.json(schmapath).schema
  
    var readJson=spark.read.schema(structSchema).json(datapath).where("events.beaconType='pageAdRequested'")
    
    val pageAdRequested= """events.client as client
    events.beaconType as beaconType
    events.beaconVersion as beaconVersion
    events.data.displayAd.instanceID as instanceid
    events.data.milestones.amazonA9Requested as amazonRequested
    events.data.milestones.amazonA9BidsRequested as amazonBidsRequested""".split("\n")
    
  var selectExpr=readJson.selectExpr(pageAdRequested:_*)

  println("****************************",selectExpr.count())
  
  var windowspec=Window.partitionBy("client","beaconType").orderBy(desc("instanceid"))
  var dedup=selectExpr.withColumn("rank", row_number over windowspec)
  println("####################",dedup.count())
  dedup.write.mode("overWrite").partitionBy("client").format("parquet").save(outputpath)
  
  }
 
}