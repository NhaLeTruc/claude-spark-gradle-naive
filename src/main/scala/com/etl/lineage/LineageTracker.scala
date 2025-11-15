package com.etl.lineage

import com.etl.core.LineageMetadata
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Lineage tracker for data provenance.
 *
 * Embeds and maintains lineage metadata throughout ETL pipelines.
 */
class LineageTracker {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  /**
   * Embed lineage metadata into DataFrame.
   *
   * Adds _lineage column containing serialized LineageMetadata.
   *
   * @param data DataFrame to add lineage to
   * @param lineage Lineage metadata
   * @return DataFrame with lineage column
   */
  def embedLineage(data: DataFrame, lineage: LineageMetadata): DataFrame = {
    val lineageJson = mapper.writeValueAsString(lineage)
    data.withColumn("_lineage", lit(lineageJson))
  }

  /**
   * Add transformation step to lineage chain.
   *
   * Updates existing lineage metadata with new transformation.
   *
   * @param data DataFrame with existing lineage
   * @param transformation Transformation description
   * @return DataFrame with updated lineage
   */
  def addTransformation(data: DataFrame, transformation: String): DataFrame = {
    if (!data.columns.contains("_lineage")) {
      // No existing lineage, cannot add transformation
      return data
    }

    // Extract existing lineage from first row
    val existingLineageJson = data.select("_lineage")
      .take(1)
      .headOption
      .map(_.getString(0))
      .getOrElse(return data) // If no lineage found, return data unchanged
    val existingLineage = mapper.readValue(existingLineageJson, classOf[LineageMetadata])

    // Update transformation chain
    val updatedLineage = existingLineage.copy(
      transformationChain = existingLineage.transformationChain :+ transformation
    )

    // Replace lineage column
    val updatedLineageJson = mapper.writeValueAsString(updatedLineage)
    data.withColumn("_lineage", lit(updatedLineageJson))
  }

  /**
   * Extract lineage metadata from DataFrame.
   *
   * @param data DataFrame with lineage column
   * @return Optional lineage metadata
   */
  def extractLineage(data: DataFrame): Option[LineageMetadata] = {
    if (!data.columns.contains("_lineage")) {
      return None
    }

    try {
      val lineageJson = data.select("_lineage")
        .take(1)
        .headOption
        .map(_.getString(0))
        .getOrElse(return None) // If no rows, return None
      Some(mapper.readValue(lineageJson, classOf[LineageMetadata]))
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Merge lineage from two DataFrames (for joins).
   *
   * Creates combined lineage showing data from both sources.
   *
   * @param left Left DataFrame with lineage
   * @param right Right DataFrame with lineage
   * @param joinType Type of join performed
   * @return Merged lineage metadata
   */
  def mergeLineage(
    left: DataFrame,
    right: DataFrame,
    joinType: String
  ): Option[LineageMetadata] = {
    val leftLineage = extractLineage(left)
    val rightLineage = extractLineage(right)

    (leftLineage, rightLineage) match {
      case (Some(l), Some(r)) =>
        // Merge transformation chains
        val mergedChain = l.transformationChain ++ r.transformationChain :+ s"join($joinType)"

        // Use left source as primary, note right source in transformation
        Some(l.copy(
          transformationChain = mergedChain
        ))

      case (Some(l), None) =>
        Some(l.copy(
          transformationChain = l.transformationChain :+ s"join($joinType)"
        ))

      case (None, Some(r)) =>
        Some(r.copy(
          transformationChain = r.transformationChain :+ s"join($joinType)"
        ))

      case (None, None) =>
        None
    }
  }

  /**
   * Get lineage summary as human-readable string.
   *
   * @param lineage Lineage metadata
   * @return Summary string
   */
  def getSummary(lineage: LineageMetadata): String = {
    val steps = lineage.transformationChain.mkString(" → ")
    s"Source: ${lineage.sourceType}/${lineage.sourceIdentifier} → $steps"
  }
}
