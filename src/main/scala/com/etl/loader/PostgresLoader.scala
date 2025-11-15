package com.etl.loader

import com.etl.core._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.sql.DriverManager

/**
 * PostgreSQL data loader.
 *
 * Writes data to PostgreSQL tables using JDBC.
 * Supports append, overwrite, and upsert modes.
 *
 * Required parameters:
 * - url: JDBC connection URL (e.g., jdbc:postgresql://localhost:5432/dbname)
 * - table: Table name to write to
 *
 * Optional parameters:
 * - user: Database username (can be retrieved from Vault)
 * - password: Database password (can be retrieved from Vault)
 * - primaryKey: Primary key column for upsert mode
 * - batchSize: JDBC batch size (default: 1000)
 */
class PostgresLoader(vaultClient: Option[Any] = None) extends DataLoader {

  override def sinkType: String = "postgres"

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

    val url = config.parameters("url")
    val table = config.parameters("table")

    // Get credentials from Vault or config
    val credentials = getCredentials(config)
    val user = credentials.getOrElse("user", config.parameters.getOrElse("user", ""))
    val password = credentials.getOrElse("password", config.parameters.getOrElse("password", ""))

    try {
      val recordCount = data.count()

      config.writeMode match {
        case "append" =>
          // Standard append
          data.write
            .format("jdbc")
            .option("url", url)
            .option("dbtable", table)
            .option("user", user)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", config.parameters.getOrElse("batchSize", "1000"))
            .mode(SaveMode.Append)
            .save()

        case "overwrite" =>
          // Truncate and insert
          data.write
            .format("jdbc")
            .option("url", url)
            .option("dbtable", table)
            .option("user", user)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", config.parameters.getOrElse("batchSize", "1000"))
            .mode(SaveMode.Overwrite)
            .save()

        case "upsert" =>
          // PostgreSQL ON CONFLICT upsert
          performUpsert(data, config, url, table, user, password)

        case other =>
          throw new LoadException(
            sinkType = sinkType,
            message = s"Unsupported write mode: $other"
          )
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
          message = s"Failed to write to PostgreSQL table '$table': ${e.getMessage}",
          cause = e
        )
    }
  }

  override def validateConfig(config: SinkConfig): ValidationResult = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    // Check required parameters
    if (!config.parameters.contains("url")) {
      errors += "Missing required parameter: url"
    } else {
      val url = config.parameters("url")
      if (!url.startsWith("jdbc:postgresql://")) {
        errors += "Invalid PostgreSQL JDBC URL (must start with 'jdbc:postgresql://')"
      }
    }

    if (!config.parameters.contains("table")) {
      errors += "Missing required parameter: table"
    }

    // Check upsert requirements
    if (config.writeMode == "upsert" && !config.parameters.contains("primaryKey")) {
      errors += "Upsert mode requires primaryKey parameter"
    }

    if (errors.isEmpty) {
      ValidationResult.valid()
    } else {
      ValidationResult.invalid(errors.toList)
    }
  }

  /**
   * Perform upsert using PostgreSQL ON CONFLICT.
   */
  private def performUpsert(
    data: DataFrame,
    config: SinkConfig,
    url: String,
    table: String,
    user: String,
    password: String
  )(implicit spark: SparkSession): Unit = {
    val primaryKey = config.parameters("primaryKey")

    // Create temp table name
    val tempTable = s"${table}_temp_${System.currentTimeMillis()}"

    // Write to temp table
    data.write
      .format("jdbc")
      .option("url", url)
      .option("dbtable", tempTable)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .option("createTableOptions", "TEMPORARY")
      .mode(SaveMode.Overwrite)
      .save()

    // Perform upsert using ON CONFLICT
    val columns = data.columns.mkString(", ")
    val updateSet = data.columns.filterNot(_ == primaryKey).map(c => s"$c = EXCLUDED.$c").mkString(", ")

    val upsertSql = s"""
      INSERT INTO $table ($columns)
      SELECT $columns FROM $tempTable
      ON CONFLICT ($primaryKey) DO UPDATE SET $updateSet
    """

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(upsertSql)
      stmt.execute(s"DROP TABLE IF EXISTS $tempTable")
      stmt.close()
    } finally {
      conn.close()
    }
  }

  /**
   * Get credentials from Vault or return empty map.
   */
  private def getCredentials(config: SinkConfig): Map[String, String] = {
    vaultClient match {
      case Some(client) =>
        try {
          val method = client.getClass.getMethod("getSecret", classOf[String])
          method.invoke(client, config.credentialsPath).asInstanceOf[Map[String, String]]
        } catch {
          case _: Exception => Map.empty
        }
      case None => Map.empty
    }
  }
}
