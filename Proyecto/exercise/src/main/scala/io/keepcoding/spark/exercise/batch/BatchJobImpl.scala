package io.keepcoding.spark.exercise.batch

import io.keepcoding.spark.exercise.batch.BatchJobImpl.totalBytesUserMail
import io.keepcoding.spark.exercise.streaming.StreamingJobImpl.spark
import org.apache.spark.sql.functions.{avg, lit, max, min, sum, window}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import java.time.OffsetDateTime

object BatchJobImpl extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load("/tmp/data-spark")
      .where(
        $"year" === lit(filterDate.getYear) &&
          $"month" === lit(filterDate.getMonthValue) &&
          $"day" === lit(filterDate.getDayOfMonth) &&
          $"hour" === lit(filterDate.getHour)
      )
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

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("a")
      .join(metadataDF.as("b"),  $"a.id" === $"b.id")
      .drop($"b.id")
  }

  override def countAntennaBytes(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"antenna_id".as("id"), $"bytes")
      .withColumn("type", lit("antenna_total_bytes"))
      .groupBy($"id", window($"timestamp", "1 hour"), $"type")
      .agg(
        sum("bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"id",  $"total_bytes".as("value"), $"type")
  }

  override def countEmailBytes(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"email".as("id"), $"bytes")
      .withColumn("type", lit("mail_total_bytes"))
      .groupBy($"id", window($"timestamp", "1 hour"), $"type")
      .agg(
        sum("bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"id",  $"total_bytes".as("value"), $"type")
  }

  override def countAppBytes(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"app".as("id"), $"bytes")
      .withColumn("type", lit("app_total_bytes"))
      .groupBy($"id", window($"timestamp", "1 hour"), $"type")
      .agg(
        sum("bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"id",  $"total_bytes".as("value"), $"type")

  }

  override def totalBytesUserMail(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"email", $"bytes", $"quota")
      .groupBy($"email", window($"timestamp", "1 hour"), $"quota")
      .agg(
        sum("bytes").as("total_bytes"),
      )
      .select($"email", $"total_bytes".as("usage"), $"quota", $"window.start".as("timestamp"))
      .where($"total_bytes" > $"quota")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = ???

  def main(args: Array[String]) = {

    val rawDF = readFromStorage("/tmp/data-spark",OffsetDateTime.now()) //OffsetDateTime.parse("2022-02-27T14:50:00Z")) //la Z es que es UTC
    val metadataDF = readAntennaMetadata(s"jdbc:postgresql://34.88.152.206:5432/postgres",
      "user_metadata",
      "postgres",
      "Keepcoding030"
    )

    val enrichDF = enrichAntennaWithMetadata(rawDF, metadataDF)

    writeToJdbc(countAntennaBytes(enrichDF),
      s"jdbc:postgresql://34.88.152.206:5432/postgres",
      "bytes_hourly",
      "postgres",
      "Keepcoding030"
    )

    writeToJdbc(countEmailBytes(enrichDF),
      s"jdbc:postgresql://34.88.152.206:5432/postgres",
      "bytes_hourly",
      "postgres",
      "Keepcoding030"
    )

    writeToJdbc(countAppBytes(enrichDF),
      s"jdbc:postgresql://34.88.152.206:5432/postgres",
      "bytes_hourly",
      "postgres",
      "Keepcoding030"
    )

    writeToJdbc(totalBytesUserMail(enrichDF),
      s"jdbc:postgresql://34.88.152.206:5432/postgres",
      "user_quota_limit",
      "postgres",
      "Keepcoding030"
    )



  }
}
