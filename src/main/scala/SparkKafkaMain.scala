import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.Seconds

object SparkKafkaMain {

  val BATH_INTERVAL = Seconds(30)
  val KAFKA_TOPIC = "mymall_feed"


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

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "myMallFeed",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def getSparkSession: SparkSession ={
    SparkSession.builder().config(getSparkConf).getOrCreate()
  }



  def startStream(kafkaBroker:String): Unit = {

    val spark = getSparkSession

    val mallDataSchema = StructType(Array(
      StructField("CustomerID", IntegerType),
      StructField("Gender", StringType),
      StructField("Age", IntegerType),
      StructField("Annual Income (k$)", IntegerType),
      StructField("Spending Score (1-100)", IntegerType)
    ))

    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", KAFKA_TOPIC)
      .load()

    df.writeStream
      .format("console")
      .option("truncate","false")
      .start()
      .awaitTermination()


  }

  def main(args: Array[String]): Unit = {
   startStream("localhost:9092")
  }


}
