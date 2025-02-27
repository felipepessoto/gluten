/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.glutenproject.execution

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

trait BaseDataSource {

  /** Returns the actual schema of this data source scan. */
  def getDataSchema: StructType

  /** Returns the required partition schema, used to generate partition column. */
  def getPartitionSchema: StructType

  /** Returns the partitions generated by this data source scan. */
  def getPartitions: Seq[InputPartition]

  /** Returns the input file paths, used to validate the partition column path */
  def getInputFilePathsInternal: Seq[String]
}
