package com.etl.transformer

import com.etl.core._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Map transformer.
 *
 * Applies column expressions to create new columns or transform existing ones.
 * Supports rename, type conversion, and computed columns.
 *
 * Required parameters:
 * - expressions: Comma-separated list of "alias:expression" pairs
 *   Example: "full_name:concat(first_name, ' ', last_name),age_int:CAST(age AS INT)"
 */
class MapTransformer extends DataTransformer {

  override def transformationType: String = "map"

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

    val expressionsStr = config.parameters("expressions")

    try {
      // Parse expressions: "alias:expression,alias2:expression2,..."
      val expressions = expressionsStr.split(",").map { exprPair =>
        val parts = exprPair.split(":", 2)
        if (parts.length != 2) {
          throw new TransformationException(
            transformationType = transformationType,
            message = s"Invalid expression format: $exprPair (expected 'alias:expression')"
          )
        }
        val alias = parts(0).trim
        val expression = parts(1).trim
        (alias, expression)
      }

      // Apply all expressions
      var result = input
      expressions.foreach { case (alias, expression) =>
        result = result.withColumn(alias, expr(expression))
      }

      // Update lineage metadata
      updateLineage(result, config, input)

    } catch {
      case e: TransformationException => throw e
      case e: Exception =>
        throw new TransformationException(
          transformationType = transformationType,
          message = s"Failed to apply map transformation: ${e.getMessage}",
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
    if (!config.parameters.contains("expressions")) {
      errors += "Missing required parameter: expressions"
    } else {
      val expressionsStr = config.parameters("expressions")
      if (expressionsStr.trim.isEmpty) {
        errors += "expressions parameter cannot be empty"
      } else {
        // Validate format of expressions
        expressionsStr.split(",").foreach { exprPair =>
          val parts = exprPair.split(":", 2)
          if (parts.length != 2) {
            errors += s"Invalid expression format: $exprPair (expected 'alias:expression')"
          }
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
    val expressionList = config.parameters("expressions").split(",").map(_.split(":")(0).trim).mkString(",")
    val transformationStep = s"$transformationType(columns=$expressionList)"
    val updatedChain = existingLineage.transformationChain :+ transformationStep

    // Create updated lineage
    val updatedLineage = existingLineage.copy(transformationChain = updatedChain)
    val updatedLineageJson = mapper.writeValueAsString(updatedLineage)

    // Add updated lineage to result
    df.withColumn("_lineage", lit(updatedLineageJson))
  }
}
