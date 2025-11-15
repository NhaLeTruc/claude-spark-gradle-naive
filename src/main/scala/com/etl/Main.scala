package com.etl

import com.etl.config.YAMLConfigParser
import com.etl.core._
import com.etl.pipeline._
import com.etl.quality.{DataQualityChecker, QuarantineWriter}
import com.etl.vault.VaultClient
import com.etl.logging.{StructuredLogger, MetricsCollector}
import org.apache.spark.sql.SparkSession
import scala.util.{Try, Success, Failure}

/**
 * Main application entry point for ETL pipeline execution.
 *
 * Orchestrates:
 * - T134: Dependency injection wiring
 * - T135: Command-line argument parsing
 * - T136: Spark session creation
 * - T137: Graceful shutdown and resource cleanup
 */
object Main {

  private val logger = StructuredLogger("com.etl.Main")

  def main(args: Array[String]): Unit = {
    // T135: Parse command-line arguments
    val config = parseArguments(args) match {
      case Some(cfg) => cfg
      case None =>
        printUsage()
        sys.exit(1)
    }

    // Generate correlation ID for this run
    val correlationId = logger.generateCorrelationId()

    logger.info("Starting ETL application", Map(
      "pipelineConfigPath" -> config.pipelineConfigPath,
      "correlationId" -> correlationId
    ), Some(correlationId))

    // Execute pipeline with proper resource management
    val exitCode = Try {
      executePipeline(config, correlationId)
    } match {
      case Success(_) =>
        logger.info("ETL application completed successfully", Map.empty, Some(correlationId))
        0
      case Failure(exception) =>
        logger.error("ETL application failed", exception, Map.empty, Some(correlationId))
        1
    }

    sys.exit(exitCode)
  }

  /**
   * Execute ETL pipeline with full dependency injection.
   */
  private def executePipeline(config: AppConfig, correlationId: String): Unit = {
    // T136: Create Spark session
    implicit val spark: SparkSession = createSparkSession(config)

    try {
      logger.info("Spark session created", Map(
        "sparkVersion" -> spark.version,
        "master" -> spark.sparkContext.master
      ), Some(correlationId))

      // T134: Dependency injection wiring
      val yamlParser = new YAMLConfigParser()
      val vaultClient = VaultClient.fromEnvironment()
      val metricsCollector = new MetricsCollector()

      // Initialize registries
      val extractorRegistry = new ExtractorRegistry()
      val transformerRegistry = new TransformerRegistry()
      val loaderRegistry = new LoaderRegistry()

      // Initialize quality components
      val qualityChecker = new DataQualityChecker()
      val quarantineWriter = new QuarantineWriter()

      // Parse pipeline configuration
      logger.info("Parsing pipeline configuration", Map(
        "configPath" -> config.pipelineConfigPath
      ), Some(correlationId))

      val pipelineConfig = yamlParser.parseFile(config.pipelineConfigPath)

      // Create run context
      val runContext = RunContext.create(
        pipelineId = pipelineConfig.pipelineId,
        sparkSession = spark
      )

      logger.info("Executing pipeline", Map(
        "pipelineId" -> runContext.pipelineId,
        "runId" -> runContext.runId
      ), Some(correlationId))

      // Execute pipeline
      val executor = new PipelineExecutor()
      val metrics = if (pipelineConfig.quality.schemaValidation || pipelineConfig.quality.nullChecks.nonEmpty) {
        executor.executeWithQuality(
          pipelineConfig,
          runContext,
          extractorRegistry,
          transformerRegistry,
          loaderRegistry,
          qualityChecker,
          quarantineWriter
        )
      } else {
        executor.execute(
          pipelineConfig,
          runContext,
          extractorRegistry,
          transformerRegistry,
          loaderRegistry
        )
      }

      // Log execution results
      logger.info("Pipeline execution completed", Map(
        "pipelineId" -> metrics.pipelineId,
        "runId" -> metrics.runId,
        "status" -> metrics.status,
        "recordsExtracted" -> metrics.recordsExtracted.toString,
        "recordsLoaded" -> metrics.recordsLoaded.toString,
        "durationSeconds" -> metrics.durationSeconds.toString
      ), Some(correlationId))

      // Record metrics
      metricsCollector.recordExtraction(
        metrics.pipelineId,
        metrics.runId,
        pipelineConfig.source.`type`,
        metrics.recordsExtracted,
        metrics.duration
      )

      metricsCollector.recordLoad(
        metrics.pipelineId,
        metrics.runId,
        pipelineConfig.sink.`type`,
        metrics.recordsLoaded,
        metrics.duration,
        pipelineConfig.sink.writeMode
      )

      if (metrics.status != "SUCCESS") {
        throw new RuntimeException(s"Pipeline failed: ${metrics.errorDetails.getOrElse("Unknown error")}")
      }

    } finally {
      // T137: Graceful shutdown and resource cleanup
      logger.info("Shutting down Spark session", Map.empty, Some(correlationId))
      spark.stop()
    }
  }

  /**
   * Create Spark session with configuration.
   */
  private def createSparkSession(config: AppConfig): SparkSession = {
    var builder = SparkSession.builder()
      .appName(config.appName)

    // Set master if provided
    config.sparkMaster.foreach { master =>
      builder = builder.master(master)
    }

    // Apply additional Spark configurations
    config.sparkConfigs.foreach { case (key, value) =>
      builder = builder.config(key, value)
    }

    builder.getOrCreate()
  }

  /**
   * Parse command-line arguments.
   */
  private def parseArguments(args: Array[String]): Option[AppConfig] = {
    if (args.isEmpty) {
      return None
    }

    var pipelineConfigPath: Option[String] = None
    var appName = "ETL Pipeline"
    var sparkMaster: Option[String] = None
    var sparkConfigs = Map.empty[String, String]

    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--pipeline" | "-p" =>
          if (i + 1 < args.length) {
            pipelineConfigPath = Some(args(i + 1))
            i += 2
          } else {
            return None
          }

        case "--app-name" =>
          if (i + 1 < args.length) {
            appName = args(i + 1)
            i += 2
          } else {
            return None
          }

        case "--master" | "-m" =>
          if (i + 1 < args.length) {
            sparkMaster = Some(args(i + 1))
            i += 2
          } else {
            return None
          }

        case "--conf" =>
          if (i + 1 < args.length) {
            val conf = args(i + 1).split("=", 2)
            if (conf.length == 2) {
              sparkConfigs = sparkConfigs + (conf(0) -> conf(1))
            }
            i += 2
          } else {
            return None
          }

        case _ =>
          println(s"Unknown argument: ${args(i)}")
          return None
      }
    }

    pipelineConfigPath.map { path =>
      AppConfig(path, appName, sparkMaster, sparkConfigs)
    }
  }

  /**
   * Print usage information.
   */
  private def printUsage(): Unit = {
    println(
      """
        |Usage: etl-spark-gradle [OPTIONS]
        |
        |Options:
        |  --pipeline, -p PATH      Path to pipeline YAML configuration (required)
        |  --app-name NAME          Spark application name (default: "ETL Pipeline")
        |  --master, -m MASTER      Spark master URL (default: from config)
        |  --conf KEY=VALUE         Spark configuration property
        |
        |Examples:
        |  # Run pipeline with local Spark
        |  etl-spark-gradle --pipeline pipelines/my-pipeline.yaml --master local[4]
        |
        |  # Run pipeline with custom Spark config
        |  etl-spark-gradle --pipeline pipelines/my-pipeline.yaml \
        |    --conf spark.executor.memory=4g \
        |    --conf spark.driver.memory=2g
        |""".stripMargin
    )
  }

  /**
   * Application configuration.
   */
  case class AppConfig(
    pipelineConfigPath: String,
    appName: String,
    sparkMaster: Option[String],
    sparkConfigs: Map[String, String]
  )
}
