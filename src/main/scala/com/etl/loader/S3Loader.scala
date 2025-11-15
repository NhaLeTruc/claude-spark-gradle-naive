package com.etl.loader

import com.etl.core._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * S3 data loader.
 *
 * Writes data to S3 (or S3-compatible storage) using Spark.
 * Supports multiple formats and partitioning.
 *
 * Required parameters:
 * - path: S3 path (e.g., s3://bucket/prefix/data)
 * - format: File format (avro, parquet, json, csv)
 *
 * Optional parameters:
 * - partitionBy: Comma-separated list of columns to partition by
 * - compression: Compression codec (gzip, snappy, lz4, none)
 * - coalesce: Number of output files
 */
class S3Loader extends DataLoader {

  override def sinkType: String = "s3"

  private val supportedFormats = Set("avro", "parquet", "json", "csv")

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

    val path = config.parameters("path")
    val format = config.parameters("format")

    try {
      val recordCount = data.count()

      // Build writer
      var writer = data.write.format(format)

      // Apply write mode
      writer = config.writeMode match {
        case "append" => writer.mode(SaveMode.Append)
        case "overwrite" => writer.mode(SaveMode.Overwrite)
        case other => throw new LoadException(
          sinkType = sinkType,
          message = s"Unsupported write mode for S3: $other (use 'append' or 'overwrite')"
        )
      }

      // Apply format-specific options
      format match {
        case "csv" =>
          writer = writer
            .option("header", config.parameters.getOrElse("header", "true"))
            .option("delimiter", config.parameters.getOrElse("delimiter", ","))

        case "json" =>
          config.parameters.get("compression").foreach { codec =>
            writer = writer.option("compression", codec)
          }

        case "avro" | "parquet" =>
          config.parameters.get("compression").foreach { codec =>
            writer = writer.option("compression", codec)
          }

        case _ =>
      }

      // Apply partitioning if specified
      config.parameters.get("partitionBy").foreach { partitionCols =>
        val cols = partitionCols.split(",").map(_.trim)
        writer = writer.partitionBy(cols: _*)
      }

      // Apply coalesce if specified
      val finalData = config.parameters.get("coalesce") match {
        case Some(numFiles) => data.coalesce(numFiles.toInt)
        case None => data
      }

      // Write to S3
      if (config.parameters.contains("coalesce")) {
        finalData.write.format(format)
          .mode(writer.mode)
          .options(writer.options)
          .save(path)
      } else {
        writer.save(path)
      }

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
          message = s"Failed to write to S3 path '$path': ${e.getMessage}",
          cause = e
        )
    }
  }

  override def validateConfig(config: SinkConfig): ValidationResult = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    // Check required parameters
    if (!config.parameters.contains("path")) {
      errors += "Missing required parameter: path"
    }

    if (!config.parameters.contains("format")) {
      errors += "Missing required parameter: format"
    } else {
      val format = config.parameters("format")
      if (!supportedFormats.contains(format)) {
        errors += s"Invalid format: $format (supported: ${supportedFormats.mkString(", ")})"
      }
    }

    if (errors.isEmpty) {
      ValidationResult.valid()
    } else {
      ValidationResult.invalid(errors.toList)
    }
  }
}
