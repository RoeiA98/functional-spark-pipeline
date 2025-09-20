package main

import org.apache.spark.sql.SparkSession

/**
 * Setup object for verifying Spark and Scala environment configuration.
 * Provides basic environment validation and version information display.
 * Demonstrates functional error handling and resource management.
 */
object Setup {

  /**
   * Main entry point for setup validation.
   * Creates SparkSession, displays version information, and performs cleanup.
   * Uses functional error handling with proper resource management.
   *
   * @param args command line arguments (not used in this implementation)
   */
  def main(args: Array[String]): Unit = {
    // Input validation - method starts with validating arguments
    require(args != null, "Arguments array cannot be null")

    /*
     * Create SparkSession with proper configuration
     * Uses builder pattern for session configuration
     */
    val spark = SparkSession.builder()
      .appName("Setup")
      .master("local[*]")
      .getOrCreate()

    try {
      /*
       * Display environment information for verification
       * Demonstrates property access and string interpolation
       */
      println("Spark version: " + spark.version)
      println("Scala version: " + scala.util.Properties.versionString)
      println("Setup successful!")

    } finally {
      // Ensure proper resource cleanup regardless of execution outcome
      spark.stop()
    }
  }
}