package com.etl.loader

import com.etl.core._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Kafka data loader.
 *
 * Writes data to Kafka topics using Spark's Kafka integration.
 * Supports multiple serialization formats.
 *
 * Required parameters:
 * - bootstrap.servers: Kafka broker addresses
 * - topic: Topic name to write to
 *
 * Optional parameters:
 * - format: Serialization format ("json" | "avro", default: "json")
 * - keyColumn: Column to use as Kafka key (default: "key")
 * - valueColumn: Column to use as Kafka value (default: "value")
 */
class KafkaLoader extends DataLoader {

  override def sinkType: String = "kafka"

  override def load(
    data: DataFrame,
    config: SinkConfig,
    runContext: RunContext
  ): LoadResult = {
    implicit val spark: SparkSession = runContext.sparkSession

    // Validate configuration first
    val validation = validateConfig(config)
    if (!validation.isValid) {
      throw new LoadException(
        sinkType = sinkType,
        message = s"Invalid configuration: ${validation.errors.mkString(", ")}"
      )
    }

    val bootstrapServers = config.parameters("bootstrap.servers")
    val topic = config.parameters("topic")
    val format = config.parameters.getOrElse("format", "json")
    val keyColumn = config.parameters.getOrElse("keyColumn", "key")
    val valueColumn = config.parameters.getOrElse("valueColumn", "value")

    try {
      // Prepare data for Kafka
      val kafkaData = if (data.columns.contains(keyColumn) && data.columns.contains(valueColumn)) {
        // Use existing key/value columns
        data.selectExpr(
          s"CAST($keyColumn AS STRING) as key",
          s"CAST($valueColumn AS STRING) as value"
        )
      } else {
        // Create key/value columns from all columns
        format match {
          case "avro" | "json" =>
            // Convert entire row to JSON
            data
              .withColumn("key", lit(null).cast("string"))
              .withColumn("value", to_json(struct(data.columns.map(col): _*)))
              .select("key", "value")
          case _ =>
            throw new LoadException(
              sinkType = sinkType,
              message = s"Unsupported format: $format (must be 'json' or 'avro')"
            )
        }
      }

      // Write to Kafka
      kafkaData.write
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("topic", topic)
        .mode(config.writeMode) // Kafka supports append and overwrite (complete)
        .save()

      val recordCount = kafkaData.count()

      LoadResult.success(
        recordsWritten = recordCount,
        sinkType = sinkType,
        writeMode = config.writeMode
      )

    } catch {
      case e: LoadException => throw e
      case e: Exception =>
        throw new LoadException(
          sinkType = sinkType,
          message = s"Failed to write to Kafka topic '$topic': ${e.getMessage}",
          cause = e
        )
    }
  }

  override def validateConfig(config: SinkConfig): ValidationResult = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    // Check required parameters
    if (!config.parameters.contains("bootstrap.servers")) {
      errors += "Missing required parameter: bootstrap.servers"
    }

    if (!config.parameters.contains("topic")) {
      errors += "Missing required parameter: topic"
    }

    // Validate format if specified
    config.parameters.get("format").foreach { fmt =>
      val supportedFormats = Set("json", "avro")
      if (!supportedFormats.contains(fmt)) {
        errors += s"Unsupported format: $fmt (supported: ${supportedFormats.mkString(", ")})"
      }
    }

    if (errors.isEmpty) {
      ValidationResult.valid()
    } else {
      ValidationResult.invalid(errors.toList)
    }
  }
}
