package com.etl.transformer

import com.etl.core._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Windowing transformer.
 *
 * Applies time-based windowing operations (tumbling or sliding windows).
 *
 * Required parameters:
 * - windowType: Type of window (tumbling | sliding)
 * - timeColumn: Column name containing timestamp
 * - windowDuration: Window duration (e.g., "10 minutes", "1 hour")
 * - slideDuration: Slide duration for sliding windows (required if windowType=sliding)
 *
 * Required config:
 * - aggregations: List of AggregateExpr to apply within windows
 */
class WindowingTransformer extends DataTransformer {

  override def transformationType: String = "windowing"

  override def transform(
    input: DataFrame,
    config: TransformationConfig,
    runContext: RunContext
  ): DataFrame = {
    // Validate configuration first
    val validation = validateConfig(config, input.schema)
    if (!validation.isValid) {
      throw new TransformationException(
        transformationType = transformationType,
        message = s"Invalid configuration: ${validation.errors.mkString(", ")}"
      )
    }

    val windowType = config.parameters("windowType")
    val timeColumn = config.parameters("timeColumn")
    val windowDuration = config.parameters("windowDuration")
    val aggregations = config.aggregations.get

    try {
      // Create window column based on window type
      val windowedDf = windowType.toLowerCase match {
        case "tumbling" =>
          input.withColumn("window", window(col(timeColumn), windowDuration))

        case "sliding" =>
          val slideDuration = config.parameters("slideDuration")
          input.withColumn("window", window(col(timeColumn), windowDuration, slideDuration))

        case other => throw new TransformationException(
          transformationType = transformationType,
          message = s"Unsupported window type: $other"
        )
      }

      // Build aggregation expressions
      val aggExprs = aggregations.map { agg =>
        val aggFunc = agg.function.toLowerCase match {
          case "sum" => sum(agg.column)
          case "avg" => avg(agg.column)
          case "count" => count(agg.column)
          case "min" => min(agg.column)
          case "max" => max(agg.column)
          case "first" => first(agg.column)
          case "last" => last(agg.column)
          case "collect_list" => collect_list(agg.column)
          case "collect_set" => collect_set(agg.column)
          case other => throw new TransformationException(
            transformationType = transformationType,
            message = s"Unsupported aggregation function: $other"
          )
        }
        aggFunc.alias(agg.alias)
      }

      // Apply windowing aggregations
      val result = windowedDf.groupBy("window")
        .agg(aggExprs.head, aggExprs.tail: _*)

      // Update lineage metadata
      updateLineage(result, config, input)

    } catch {
      case e: TransformationException => throw e
      case e: Exception =>
        throw new TransformationException(
          transformationType = transformationType,
          message = s"Failed to apply windowing: ${e.getMessage}",
          cause = e
        )
    }
  }

  override def validateConfig(
    config: TransformationConfig,
    inputSchema: StructType
  ): ValidationResult = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    // Check required parameters
    if (!config.parameters.contains("windowType")) {
      errors += "Missing required parameter: windowType"
    } else {
      val windowType = config.parameters("windowType").toLowerCase
      val supportedTypes = Set("tumbling", "sliding")
      if (!supportedTypes.contains(windowType)) {
        errors += s"Unsupported window type: $windowType (supported: ${supportedTypes.mkString(", ")})"
      }

      // Check slideDuration for sliding windows
      if (windowType == "sliding" && !config.parameters.contains("slideDuration")) {
        errors += "Sliding window requires slideDuration parameter"
      }
    }

    if (!config.parameters.contains("timeColumn")) {
      errors += "Missing required parameter: timeColumn"
    } else {
      val timeColumn = config.parameters("timeColumn")
      if (!inputSchema.fieldNames.contains(timeColumn)) {
        errors += s"Time column not found in schema: $timeColumn"
      }
    }

    if (!config.parameters.contains("windowDuration")) {
      errors += "Missing required parameter: windowDuration"
    }

    // Check aggregations list
    if (config.aggregations.isEmpty) {
      errors += "Missing aggregations list"
    } else {
      val schemaFields = inputSchema.fieldNames.toSet
      config.aggregations.get.foreach { agg =>
        // Validate column exists (except for count(*))
        if (agg.column != "*" && !schemaFields.contains(agg.column)) {
          errors += s"Aggregation column not found in schema: ${agg.column}"
        }

        // Validate function is supported
        val supportedFunctions = Set("sum", "avg", "count", "min", "max", "first", "last", "collect_list", "collect_set")
        if (!supportedFunctions.contains(agg.function.toLowerCase)) {
          errors += s"Unsupported aggregation function: ${agg.function}"
        }
      }
    }

    if (errors.isEmpty) {
      ValidationResult.valid()
    } else {
      ValidationResult.invalid(errors.toList)
    }
  }

  /**
   * Update lineage metadata with transformation details.
   */
  private def updateLineage(
    df: DataFrame,
    config: TransformationConfig,
    input: DataFrame
  )(implicit spark: SparkSession): DataFrame = {
    if (!input.columns.contains("_lineage")) {
      return df
    }

    // Parse existing lineage from first row
    val existingLineageJson = input.select("_lineage")
      .take(1)
      .headOption
      .map(_.getString(0))
      .getOrElse(throw new TransformationException(
        transformationType = transformationType,
        message = "Missing lineage metadata in input DataFrame"
      ))
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val existingLineage = mapper.readValue(existingLineageJson, classOf[LineageMetadata])

    // Create new transformation step
    val transformationStep = s"$transformationType(type=${config.parameters("windowType")},duration=${config.parameters("windowDuration")})"
    val updatedChain = existingLineage.transformationChain :+ transformationStep

    // Create updated lineage
    val updatedLineage = existingLineage.copy(transformationChain = updatedChain)
    val updatedLineageJson = mapper.writeValueAsString(updatedLineage)

    // Add updated lineage to result
    df.withColumn("_lineage", lit(updatedLineageJson))
  }
}
