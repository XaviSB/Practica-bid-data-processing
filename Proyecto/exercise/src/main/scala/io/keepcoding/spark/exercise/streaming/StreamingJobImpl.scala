package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object  StreamingJobImpl extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val struct = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false),
      StructField("bytes", LongType, nullable = false),
      StructField("app", StringType, nullable = false),
    ))

    dataFrame
      .select(from_json($"value".cast(StringType), struct).as("value"))
      .select($"value.*")

  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame =  {
    antennaDF.as("a")
      .join(metadataDF.as("b"),  $"a.id" === $"b.id")
      .drop($"b.id")
  }

  override def countAntennaBytes(dataFrame: DataFrame): DataFrame =   {
    dataFrame
      .select($"timestamp", $"antenna_id".as("id"), $"bytes")
      .withColumn("type", lit("antenna_total_bytes"))
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"id", window($"timestamp", "5 minutes"), $"type")
      .agg(
        sum("bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"id",  $"total_bytes".as("value"), $"type")
  }

  override def countUserBytes(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"bytes")
      .withColumn("type", lit("user_total_bytes"))
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"id", window($"timestamp", "5 minutes"), $"type")
      .agg(
        sum("bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"id",  $"total_bytes".as("value"), $"type")
  }

  override def countAppBytes(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"app".as("id"), $"bytes")
      .withColumn("type", lit("app_total_bytes"))
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"id", window($"timestamp", "5 minutes"), $"type")
      .agg(
        sum("bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"id",  $"total_bytes".as("value"), $"type")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }.start()
      .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] =  Future {
    dataFrame
      .withColumn("year", year($"timestamp"))
      .withColumn("month", month($"timestamp"))
      .withColumn("day", dayofmonth($"timestamp"))
      .withColumn("hour", hour($"timestamp"))
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", storageRootPath)
      .option("checkpointLocation", "/tmp/spark-checkpoint4")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
  /* readFromKafka("34.79.236.105:9092", "devices")
      .writeStream
      .format("console")
      .start()
      .awaitTermination() */ //para probar que funciona

   /* parserJsonData(readFromKafka("34.79.236.105", "devices"))
      .writeStream
      .format("console")
      .start()
      .awaitTermination() */ //para probar el parseo.

      val metadataDF = readAntennaMetadata(
      s"jdbc:postgresql://34.88.152.206:5432/postgres",
      "user_metadata",
      "postgres",
      "Keepcoding030")


   /*/  enrichAntennaWithMetadata(parserJsonData(readFromKafka("34.79.236.105:9092","devices")), metadataDF)
      .writeStream
      .format("console")
      .start()
      .awaitTermination() */

  /*    countAntennaBytes(enrichAntennaWithMetadata(parserJsonData(readFromKafka("34.79.236.105:9092","devices")), metadataDF))
      .writeStream
      .format("console")
      .start()
      .awaitTermination()*/

   /*   countUserBytes(enrichAntennaWithMetadata(parserJsonData(readFromKafka("34.79.236.105:9092","devices")), metadataDF))
      .writeStream
      .format("console")
      .start()
      .awaitTermination()*/

   /*   countAppBytes(enrichAntennaWithMetadata(parserJsonData(readFromKafka("34.79.236.105:9092","devices")), metadataDF))
      .writeStream
      .format("console")
      .start()
      .awaitTermination() */

    val future1 = writeToJdbc(countAntennaBytes(
      enrichAntennaWithMetadata(
        parserJsonData(
          readFromKafka("34.79.236.105:9092", "devices")
        ),
        metadataDF
      )
    ), s"jdbc:postgresql://34.88.152.206:5432/postgres", "bytes", "postgres", "Keepcoding030")

    val future2 = writeToJdbc(countUserBytes(
      enrichAntennaWithMetadata(
        parserJsonData(
          readFromKafka("34.79.236.105:9092", "devices")
        ),
        metadataDF
      )
    ), s"jdbc:postgresql://34.88.152.206:5432/postgres", "bytes", "postgres", "Keepcoding030")

    val future3 = writeToJdbc(countAppBytes(
      enrichAntennaWithMetadata(
        parserJsonData(
          readFromKafka("34.79.236.105:9092", "devices")
        ),
        metadataDF
      )
    ), s"jdbc:postgresql://34.88.152.206:5432/postgres", "bytes", "postgres", "Keepcoding030")

    val future4 = writeToStorage(parserJsonData(readFromKafka("34.79.236.105:9092", "devices")), "/tmp/data-spark")

    Await.result(Future.sequence(Seq(future1, future2, future3, future4)), Duration.Inf)
  }


}
