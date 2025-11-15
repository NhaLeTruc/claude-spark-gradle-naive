package com.etl.transformer

import com.etl.core._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Join transformer.
 *
 * Joins two DataFrames based on specified keys and join type.
 *
 * Required parameters:
 * - joinType: Type of join (inner, left, right, full)
 * - leftKey: Column name in left DataFrame
 * - rightKey: Column name in right DataFrame
 * - rightTable: Name of temp view containing right DataFrame
 *
 * Supported join types:
 * - inner: Inner join (matching rows only)
 * - left: Left outer join (all left rows)
 * - right: Right outer join (all right rows)
 * - full: Full outer join (all rows from both)
 */
class JoinTransformer extends DataTransformer {

  override def transformationType: String = "join"

  override def transform(
    input: DataFrame,
    config: TransformationConfig,
    runContext: RunContext
  ): DataFrame = {
    implicit val spark: SparkSession = runContext.sparkSession

    // Validate configuration first
    val validation = validateConfig(config, input.schema)
    if (!validation.isValid) {
      throw new TransformationException(
        transformationType = transformationType,
        message = s"Invalid configuration: ${validation.errors.mkString(", ")}"
      )
    }

    val joinType = config.parameters("joinType")
    val leftKey = config.parameters("leftKey")
    val rightKey = config.parameters("rightKey")
    val rightTableName = config.parameters.getOrElse("rightTable", "right_table")

    try {
      // Load right DataFrame from temp view
      val rightDf = spark.table(rightTableName)

      // Perform join
      val joinCondition = input(leftKey) === rightDf(rightKey)
      val result = joinType.toLowerCase match {
        case "inner" => input.join(rightDf, joinCondition, "inner")
        case "left" => input.join(rightDf, joinCondition, "left")
        case "right" => input.join(rightDf, joinCondition, "right")
        case "full" => input.join(rightDf, joinCondition, "full")
        case other => throw new TransformationException(
          transformationType = transformationType,
          message = s"Unsupported join type: $other"
        )
      }

      // Handle duplicate columns from join (keep left side columns, drop right side duplicates)
      val leftCols = input.columns.toSet
      val rightCols = rightDf.columns.toSet
      val duplicates = leftCols.intersect(rightCols)

      val finalResult = if (duplicates.nonEmpty) {
        // Drop duplicate columns from right side
        val selectCols = input.columns.map(c => input(c)) ++
          rightDf.columns.filterNot(duplicates.contains).map(c => rightDf(c))
        result.select(selectCols: _*)
      } else {
        result
      }

      // Update lineage metadata
      updateLineage(finalResult, config, input, rightDf)

    } catch {
      case e: TransformationException => throw e
      case e: Exception =>
        throw new TransformationException(
          transformationType = transformationType,
          message = s"Failed to apply join: ${e.getMessage}",
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
    if (!config.parameters.contains("joinType")) {
      errors += "Missing required parameter: joinType"
    } else {
      val joinType = config.parameters("joinType").toLowerCase
      val supportedTypes = Set("inner", "left", "right", "full")
      if (!supportedTypes.contains(joinType)) {
        errors += s"Unsupported join type: $joinType (supported: ${supportedTypes.mkString(", ")})"
      }
    }

    if (!config.parameters.contains("leftKey")) {
      errors += "Missing required parameter: leftKey"
    } else {
      val leftKey = config.parameters("leftKey")
      if (!inputSchema.fieldNames.contains(leftKey)) {
        errors += s"Left join key not found in schema: $leftKey"
      }
    }

    if (!config.parameters.contains("rightKey")) {
      errors += "Missing required parameter: rightKey"
    }

    if (errors.isEmpty) {
      ValidationResult.valid()
    } else {
      ValidationResult.invalid(errors.toList)
    }
  }

  /**
   * Update lineage metadata with join transformation details.
   */
  private def updateLineage(
    df: DataFrame,
    config: TransformationConfig,
    leftDf: DataFrame,
    rightDf: DataFrame
  )(implicit spark: SparkSession): DataFrame = {
    if (!leftDf.columns.contains("_lineage")) {
      return df
    }

    // Parse existing lineage from left DataFrame
    val leftLineageJson = leftDf.select("_lineage").first().getString(0)
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val leftLineage = mapper.readValue(leftLineageJson, classOf[LineageMetadata])

    // Create new transformation step
    val transformationStep = s"$transformationType(type=${config.parameters("joinType")},leftKey=${config.parameters("leftKey")},rightKey=${config.parameters("rightKey")})"
    val updatedChain = leftLineage.transformationChain :+ transformationStep

    // Create updated lineage
    val updatedLineage = leftLineage.copy(transformationChain = updatedChain)
    val updatedLineageJson = mapper.writeValueAsString(updatedLineage)

    // Add updated lineage to result
    df.withColumn("_lineage", lit(updatedLineageJson))
  }
}
