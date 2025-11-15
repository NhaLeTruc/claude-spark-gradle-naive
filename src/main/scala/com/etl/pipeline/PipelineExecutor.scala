package com.etl.pipeline

import com.etl.core._
import org.apache.spark.sql.DataFrame

/**
 * Pipeline executor orchestrates ETL pipeline execution.
 *
 * Coordinates extraction, transformation, and loading phases.
 */
class PipelineExecutor {

  /**
   * Execute complete ETL pipeline.
   *
   * @param config Pipeline configuration
   * @param runContext Execution context
   * @param extractorRegistry Extractor registry
   * @param transformerRegistry Transformer registry
   * @param loaderRegistry Loader registry
   * @return Execution metrics
   */
  def execute(
    config: PipelineConfig,
    runContext: RunContext,
    extractorRegistry: ExtractorRegistry,
    transformerRegistry: TransformerRegistry,
    loaderRegistry: LoaderRegistry
  ): ExecutionMetrics = {
    val startTime = System.currentTimeMillis()

    try {
      // Phase 1: Extract
      val extractor = extractorRegistry.get(config.source.`type`)
      val extractedData = extractor.extract(config.source)(runContext.sparkSession)
      val recordsExtracted = extractedData.count()

      // Phase 2: Transform
      var transformedData = extractedData
      config.transformations.foreach { transformConfig =>
        val transformer = transformerRegistry.get(transformConfig.`type`)
        transformedData = transformer.transform(transformedData, transformConfig, runContext)
      }
      val recordsTransformed = transformedData.count()

      // Phase 3: Load
      val loader = loaderRegistry.get(config.sink.`type`)
      val loadResult = loader.load(transformedData, config.sink, runContext)
      val recordsLoaded = loadResult.recordsWritten

      val endTime = System.currentTimeMillis()

      ExecutionMetrics(
        pipelineId = config.pipelineId,
        runId = runContext.runId,
        startTimestamp = startTime,
        endTimestamp = endTime,
        recordsExtracted = recordsExtracted,
        recordsTransformed = recordsTransformed,
        recordsLoaded = recordsLoaded,
        recordsFailed = 0,
        status = "SUCCESS",
        errorDetails = None
      )

    } catch {
      case e: Exception =>
        val endTime = System.currentTimeMillis()

        ExecutionMetrics(
          pipelineId = config.pipelineId,
          runId = runContext.runId,
          startTimestamp = startTime,
          endTimestamp = endTime,
          recordsExtracted = 0,
          recordsTransformed = 0,
          recordsLoaded = 0,
          recordsFailed = 0,
          status = "FAILED",
          errorDetails = Some(s"${e.getClass.getSimpleName}: ${e.getMessage}")
        )
    }
  }

  /**
   * Execute pipeline with quality checks.
   *
   * Includes data validation and quarantine handling.
   */
  def executeWithQuality(
    config: PipelineConfig,
    runContext: RunContext,
    extractorRegistry: ExtractorRegistry,
    transformerRegistry: TransformerRegistry,
    loaderRegistry: LoaderRegistry,
    qualityChecker: com.etl.quality.DataQualityChecker,
    quarantineWriter: com.etl.quality.QuarantineWriter
  ): ExecutionMetrics = {
    val startTime = System.currentTimeMillis()

    try {
      // Extract
      val extractor = extractorRegistry.get(config.source.`type`)
      val extractedData = extractor.extract(config.source)(runContext.sparkSession)

      // Quality check
      val (validData, invalidData) = qualityChecker.splitValidInvalid(extractedData, config.quality)

      // Quarantine invalid records
      if (invalidData.count() > 0) {
        quarantineWriter.writeQuarantine(
          invalidData,
          s"/quarantine/${config.pipelineId}",
          config.pipelineId,
          runContext.runId
        )(runContext.sparkSession)
      }

      val recordsExtracted = extractedData.count()
      val recordsFailed = invalidData.count()

      // Transform valid data
      var transformedData = validData
      config.transformations.foreach { transformConfig =>
        val transformer = transformerRegistry.get(transformConfig.`type`)
        transformedData = transformer.transform(transformedData, transformConfig, runContext)
      }

      // Load
      val loader = loaderRegistry.get(config.sink.`type`)
      val loadResult = loader.load(transformedData, config.sink, runContext)

      val endTime = System.currentTimeMillis()

      ExecutionMetrics(
        pipelineId = config.pipelineId,
        runId = runContext.runId,
        startTimestamp = startTime,
        endTimestamp = endTime,
        recordsExtracted = recordsExtracted,
        recordsTransformed = transformedData.count(),
        recordsLoaded = loadResult.recordsWritten,
        recordsFailed = recordsFailed,
        status = "SUCCESS",
        errorDetails = None
      )

    } catch {
      case e: Exception =>
        val endTime = System.currentTimeMillis()

        ExecutionMetrics(
          pipelineId = config.pipelineId,
          runId = runContext.runId,
          startTimestamp = startTime,
          endTimestamp = endTime,
          recordsExtracted = 0,
          recordsTransformed = 0,
          recordsLoaded = 0,
          recordsFailed = 0,
          status = "FAILED",
          errorDetails = Some(s"${e.getClass.getSimpleName}: ${e.getMessage}")
        )
    }
  }
}
