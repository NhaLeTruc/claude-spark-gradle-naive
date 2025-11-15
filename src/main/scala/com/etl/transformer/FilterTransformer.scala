package com.etl.transformer

import com.etl.core._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Filter transformer.
 *
 * Applies SQL-based filtering to DataFrames.
 *
 * Required parameters:
 * - condition: SQL WHERE condition (e.g., "age > 30 AND status = 'active'")
 */
class FilterTransformer extends DataTransformer {

  override def transformationType: String = "filter"

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

    val condition = config.parameters("condition")

    try {
      // Apply filter using SQL expression
      val result = input.filter(expr(condition))

      // Update lineage metadata
      updateLineage(result, config, input)

    } catch {
      case e: TransformationException => throw e
      case e: Exception =>
        throw new TransformationException(
          transformationType = transformationType,
          message = s"Failed to apply filter: ${e.getMessage}",
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
    if (!config.parameters.contains("condition")) {
      errors += "Missing required parameter: condition"
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
    val transformationStep = s"$transformationType(condition=${config.parameters("condition")})"
    val updatedChain = existingLineage.transformationChain :+ transformationStep

    // Create updated lineage
    val updatedLineage = existingLineage.copy(transformationChain = updatedChain)
    val updatedLineageJson = mapper.writeValueAsString(updatedLineage)

    // Add updated lineage to result
    df.withColumn("_lineage", lit(updatedLineageJson))
  }
}
