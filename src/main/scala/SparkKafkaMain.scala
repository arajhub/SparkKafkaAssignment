import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds

object SparkKafkaMain {

  val BATH_INTERVAL = Seconds(30)
  val KAFKA_TOPIC = "mymall_feed"
  val DISTANCE_THRESHOLD = 2 // 2 miles
  val storeLocation = new Location("MyMallLocation", 12.9716, 77.5946)


  def getSparkConf: SparkConf = {
    new SparkConf(true).setMaster("local").setAppName("MyMallFeedSparkStreamingApp")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.kryoserializer.buffer", "512")
      .set("spark.sql.shuffle.partitions", "200")
      .set("spark.sql.parquet.filterPushdown", "true")
      .set("spark.sql.hive.convertMetastoreParquet", "false")
      .set("spark.sql.hive.convertMetastoreOrc", "true")
  }


  def getSparkSession: SparkSession ={
    SparkSession.builder().config(getSparkConf).getOrCreate()
  }

  def startStream(kafkaBroker:String): Unit = {

    val spark = getSparkSession

    val custDataSchema = StructType(Array(
      StructField("CustomerID", IntegerType),
      StructField("Gender", StringType),
      StructField("Age", IntegerType),
      StructField("Annual Income (k$)", IntegerType),
      StructField("Spending Score (1-100)", IntegerType)
    ))

    val feedSchema = StructType(Array(
      StructField("event_name", StringType),
      StructField("event_time", StringType),
      StructField("lat", StringType),
      StructField("lon", StringType),
      StructField("cust_id", StringType),
      StructField("device_id", StringType)
    ))


    val streamDf = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", KAFKA_TOPIC)
      .load()

    val customerDf = spark.read.option("header","true").schema(custDataSchema).csv("src/test/resources/consumer.xlsx - Mall_Customers.csv")

    customerDf.persist(StorageLevel.MEMORY_AND_DISK)

    customerDf.show(truncate = false)
    import org.apache.spark.sql.functions.{col, udf}
    import spark.implicits._

    val distance = udf{
      (lat:String,lon:String) => {
        val latDouble = lat.toDouble
        val lonDouble = lon.toDouble
        val custLoc = new Location("CustLocation",latDouble, lonDouble)
        custLoc.distanceTo(storeLocation)
      }
    }

    val generateCouponDiscount = udf{
      (gender:String,income:String,spendingScore:String) => {
        val incomeInt = income.toInt
        val spendingScoreInt = spendingScore.toInt
        if (spendingScoreInt > 80 && incomeInt > 80) 50
        else if (spendingScoreInt > 60 && incomeInt > 60 && gender == "Female") 40
        else if (spendingScoreInt > 60 && incomeInt > 60 && gender == "Male") 30
        else if (gender == "Female") 25
        else 10
      }
    }


    streamDf.selectExpr("CAST(value AS STRING)").as[String]
      .select("value")
      .withColumn("jsonData",from_json(col("value"),feedSchema))
      .select("jsonData.*")
      .join(customerDf,col("cust_id") === col("CustomerID"))
      .drop(col("cust_id"))
      .withColumn("distance", distance(col("lat"), col("lon")))
      .filter(col("distance") < DISTANCE_THRESHOLD )
      .withColumn("discount", generateCouponDiscount(col("Gender"),col("Annual Income (k$)"),col("Spending Score (1-100)")))
      .select("CustomerID", "device_id", "discount").toJSON
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic","nearby_discount")
      .option("checkpointLocation", "/tmp/sparkKafkaAssignment/checkpoint") // <-- checkpoint directory
      .start()

    //To test
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", "nearby_discount")
      .load().selectExpr("CAST(value AS STRING)").as[String]
      .writeStream
      .format("console")
      .option("truncate","false")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
   startStream("localhost:9092")
  }


}
