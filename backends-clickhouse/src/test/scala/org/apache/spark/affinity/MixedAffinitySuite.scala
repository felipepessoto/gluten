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
package org.apache.spark.affinity

import io.glutenproject.execution.{GlutenMergeTreePartition, GlutenPartition}
import io.glutenproject.softaffinity.AffinityManager
import io.glutenproject.substrait.plan.PlanBuilder

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class MixedAffinitySuite extends QueryTest with SharedSparkSession {

  test("usingSoftAffinity = false return empty") {

    val manager = new AffinityManager {
      override lazy val usingSoftAffinity: Boolean = false
    }
    val affinity = new MixedAffinity(manager) {
      override def affinityMode: String = "FORCE"
    }
    val filePath = "file:///tmp/1"
    val locations = affinity.getHostLocations(filePath)
    assert(locations.isEmpty)
  }

  test("usingSoftAffinity = true and affinityMode = force return forced host") {
    val manager = new AffinityManager {
      override lazy val usingSoftAffinity: Boolean = true
    }
    manager.handleExecutorAdded(("0", "host-0"))
    manager.handleExecutorAdded(("1", "host-0"))
    val affinity = new MixedAffinity(manager) {
      override def affinityMode: String = "force"
    }
    val partition = GlutenMergeTreePartition(0, "", "", "", "fakePath", 0, 0)
    val locations = affinity.getNativeMergeTreePartitionLocations(partition)
    val nativePartition = GlutenPartition(0, PlanBuilder.EMPTY_PLAN, locations)
    assertResult(Set("forced_host_host-0")) {
      nativePartition.preferredLocations().toSet
    }
  }

  test("usingSoftAffinity = true and affinityMode = soft return cached host") {
    val manager = new AffinityManager {
      override lazy val usingSoftAffinity: Boolean = true
    }
    manager.handleExecutorAdded(("1", "host-1"))
    val affinity = new MixedAffinity(manager) {
      override def affinityMode: String = "soft"
    }
    val filePath = "file:///tmp/1"
    val locations = affinity.getHostLocations(filePath)

    assertResult(Set("executor_host-1_1")) {
      locations.toSet
    }
  }

}
