import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object straming_readjson {
    def main(args : Array[String]): Unit = {  
      
/*    var schmapath= "D:\\jsonfiles\\batch35_schema.json"
    var datapath="D:\\jsonfiles\\data\\"
    var chkpointdir="D:\\jsonfiles\\checkdir"
    var output="D:\\jsonfiles\\output\\"*/
      
    val spark=SparkSession.builder().master("local[*]").appName("Readjson").getOrCreate()
    spark.conf.set("spark.sql.straming.checkpointLocation","D:\\jsonfiles\\checkdir")
    
    var structSchema=spark.read.json("D:\\jsonfiles\\batch35_schema.json").schema
  
    var readJson=spark.readStream.schema(structSchema).json("D:\\Hadoop_iquiz\\jsonfiles\\batch34\\inputdata").where("events.beaconType='pageAdRequested'")
   
    
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
  //println("####################",dedup.count())
  //selectExpr.writeStream.outputMode("Append").format("csv").option("path",output).start().awaitTermination()
  //selcectExptDF.writeStream.outputMode("Append").format("csv").option("path", resultpath).start().awaitTermination()
  selectExpr.writeStream.outputMode("Append").format("csv").option("path","D:\\jsonfiles\\output").start().awaitTermination()
 
  }
}

