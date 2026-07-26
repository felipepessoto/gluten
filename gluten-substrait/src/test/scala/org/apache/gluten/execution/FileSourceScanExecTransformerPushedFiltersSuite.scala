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
package org.apache.gluten.execution

import org.scalatest.funsuite.AnyFunSuite

class FileSourceScanExecTransformerPushedFiltersSuite extends AnyFunSuite {

  import FileSourceScanExecTransformerBase._

  test("starPushedFilters marks every top-level entry") {
    assert(
      starPushedFilters("[IsNotNull(id), LessThan(id,5)]") ===
        "[*IsNotNull(id), *LessThan(id,5)]")
    assert(starPushedFilters("[IsNotNull(id)]") === "[*IsNotNull(id)]")
  }

  test("starPushedFilters keeps column names containing spaces intact") {
    assert(
      starPushedFilters("[IsNotNull(id with space), LessThan(id with space,5)]") ===
        "[*IsNotNull(id with space), *LessThan(id with space,5)]")
  }

  test("starPushedFilters does not split on commas nested inside an entry") {
    assert(
      starPushedFilters("[In(id, [1,2,3]), IsNotNull(x)]") ===
        "[*In(id, [1,2,3]), *IsNotNull(x)]")
  }

  test("starPushedFilters leaves an empty or non-list value unchanged") {
    assert(starPushedFilters("[]") === "[]")
    assert(starPushedFilters("") === "")
    assert(starPushedFilters("not a list") === "not a list")
  }

  test("markPushedFilters rewrites only the PushedFilters list") {
    val rendered =
      "Output [2]: [id, v]\n" +
        "DataFilters: [isnotnull(id#1), (id#1 < 5)]\n" +
        "PushedFilters: [IsNotNull(id), LessThan(id,5)]\n" +
        "ReadSchema: struct<id:int>"
    val marked = markPushedFilters(rendered)
    assert(marked.contains("PushedFilters: [*IsNotNull(id), *LessThan(id,5)]"))
    // Neighbouring metadata entries must not be touched.
    assert(marked.contains("DataFilters: [isnotnull(id#1), (id#1 < 5)]"))
    assert(marked.contains("ReadSchema: struct<id:int>"))
  }

  test("markPushedFilters leaves text without a PushedFilters list unchanged") {
    val rendered = "Output [1]: [id]\nReadSchema: struct<id:int>"
    assert(markPushedFilters(rendered) === rendered)
  }

  test("markPushedFilters is a no-op when the list is unbalanced") {
    // Here a filter value carries a stray '(' (think of a column named `a(b`), so the depth
    // counter is still above zero at the closing ']' and the scan never identifies the list end.
    // The brackets themselves are balanced; it is the parenthesis that is left open. The renderer
    // must degrade to leaving the text untouched rather than corrupting it.
    val rendered = "PushedFilters: [EqualTo(name,a(b), IsNotNull(x)]\nReadSchema: struct<x:int>"
    assert(markPushedFilters(rendered) === rendered)
  }

  test("markPushedFilters never drops or duplicates surrounding text") {
    val rendered = "A: 1\nPushedFilters: [IsNotNull(id)]\nB: 2"
    assert(markPushedFilters(rendered) === "A: 1\nPushedFilters: [*IsNotNull(id)]\nB: 2")
  }
}
