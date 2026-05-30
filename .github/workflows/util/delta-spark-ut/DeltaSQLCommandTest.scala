/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.catalog.DeltaCatalog
import io.delta.sql.DeltaSparkSessionExtension

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * A trait for tests that are testing a fully set up SparkSession with all of Delta's requirements,
 * such as the configuration of the DeltaCatalog and the addition of all Delta extensions.
 *
 * This file is injected by Gluten's delta_spark_ut.yml workflow. It mirrors the upstream
 * DeltaSQLCommandTest trait and additionally enables the Gluten plugin and the Velox-backed
 * Delta configuration that mirror
 * backends-velox/src-delta40/test/scala/org/apache/spark/sql/delta/test/DeltaSQLCommandTest.scala
 * from the apache/gluten repository.
 *
 * String keys are used on purpose so the file compiles without a build-time dependency on
 * Gluten's own config classes; the keys are loaded at runtime from the Gluten bundle jar that
 * the workflow places on the test classpath.
 */
trait DeltaSQLCommandTest extends SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
        classOf[DeltaCatalog].getName)

    // Gluten Velox backend configuration.
    conf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set(SQLConf.ANSI_ENABLED.key, "false")
      .set("spark.gluten.sql.ansiFallback.enabled", "false")
      .set("spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite", "true")
      .set("spark.databricks.delta.snapshotPartitions", "2")
      .set("spark.gluten.sql.fallbackUnexpectedMetadataParquet", "true")
  }
}
