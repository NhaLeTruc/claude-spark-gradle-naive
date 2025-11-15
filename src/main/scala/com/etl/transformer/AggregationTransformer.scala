package com.etl.transformer

import com.etl.core._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Aggregation transformer.
 *
 * Applies groupBy and aggregation functions to DataFrames.
 *
 * Required parameters:
 * - groupBy: Comma-separated list of columns to group by
 *
 * Required config:
 * - aggregations: List of AggregateExpr (column, function, alias)
 *
 * Supported aggregation functions:
 * - sum, avg, count, min, max, first, last, collect_list, collect_set
 */
class AggregationTransformer extends DataTransformer {

  override def transformationType: String = "aggregation"

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

    val groupByCols = config.parameters("groupBy").split(",").map(_.trim)
    val aggregations = config.aggregations.get

    try {
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

      // Apply groupBy and aggregations
      val result = input.groupBy(groupByCols.map(col): _*)
        .agg(aggExprs.head, aggExprs.tail: _*)

      // Update lineage metadata
      updateLineage(result, config, input)

    } catch {
      case e: TransformationException => throw e
      case e: Exception =>
        throw new TransformationException(
          transformationType = transformationType,
          message = s"Failed to apply aggregation: ${e.getMessage}",
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
    if (!config.parameters.contains("groupBy")) {
      errors += "Missing required parameter: groupBy"
    } else {
      // Validate groupBy columns exist in schema
      val groupByCols = config.parameters("groupBy").split(",").map(_.trim)
      val schemaFields = inputSchema.fieldNames.toSet
      groupByCols.foreach { col =>
        if (!schemaFields.contains(col)) {
          errors += s"GroupBy column not found in schema: $col"
        }
      }
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

    // Parse existing lineage from first row (assuming all rows have same lineage at this stage)
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
    val transformationStep = s"$transformationType(groupBy=${config.parameters("groupBy")})"
    val updatedChain = existingLineage.transformationChain :+ transformationStep

    // Create updated lineage
    val updatedLineage = existingLineage.copy(transformationChain = updatedChain)
    val updatedLineageJson = mapper.writeValueAsString(updatedLineage)

    // Add updated lineage to result
    df.withColumn("_lineage", lit(updatedLineageJson))
  }
}
