#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Prepares a delta-io/delta clone for running its `spark` module tests with the
# Gluten (Velox) bundle jar on the classpath.
#
# Usage:
#   setup-delta.sh <delta_ref> <delta_dir> <gluten_bundle_jar> <gluten_workflow_util_dir>
#
# Arguments:
#   delta_ref                 - git ref (tag/branch/sha) to check out (e.g. v4.2.0)
#   delta_dir                 - destination directory for the Delta clone
#   gluten_bundle_jar         - path to the gluten-velox-bundle fat jar
#   gluten_workflow_util_dir  - path to .github/workflows/util/delta-spark-ut/
#                               (the directory that contains DeltaSQLCommandTest.scala)
#

set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <delta_ref> <delta_dir> <gluten_bundle_jar> <gluten_workflow_util_dir>" >&2
  exit 1
fi

DELTA_REF="$1"
DELTA_DIR="$2"
GLUTEN_BUNDLE_JAR="$3"
UTIL_DIR="$4"

if [ ! -f "$GLUTEN_BUNDLE_JAR" ]; then
  echo "Gluten bundle jar not found: $GLUTEN_BUNDLE_JAR" >&2
  exit 1
fi

PATCH_SOURCE="$UTIL_DIR/DeltaSQLCommandTest.scala"
if [ ! -f "$PATCH_SOURCE" ]; then
  echo "Patched DeltaSQLCommandTest not found: $PATCH_SOURCE" >&2
  exit 1
fi

echo "::group::Cloning delta-io/delta @ ${DELTA_REF}"
# Shallow clone the requested tag/branch. Fall back to full clone when the ref is a SHA.
if ! git clone --depth 1 --branch "$DELTA_REF" https://github.com/delta-io/delta.git "$DELTA_DIR"; then
  echo "Shallow clone of ref '${DELTA_REF}' failed, falling back to full clone."
  rm -rf "$DELTA_DIR"
  git clone https://github.com/delta-io/delta.git "$DELTA_DIR"
  git -C "$DELTA_DIR" checkout "$DELTA_REF"
fi
git -C "$DELTA_DIR" --no-pager log -1 --oneline
echo "::endgroup::"

echo "::group::Injecting Gluten bundle jar into Delta sbt unmanaged jars"
# The unified `spark` sbt project lives at spark-unified/ in the Delta repo.
# sbt's default `unmanagedBase` is `<baseDirectory>/lib`, so dropping a jar in
# spark-unified/lib/ adds it to the spark project's Compile/Test classpath
# without any build.sbt change. We also place it under spark/lib/ as a safety
# net in case any helper task is invoked on the sparkV1 project directly.
SPARK_UNIFIED_LIB="$DELTA_DIR/spark-unified/lib"
SPARK_V1_LIB="$DELTA_DIR/spark/lib"
mkdir -p "$SPARK_UNIFIED_LIB" "$SPARK_V1_LIB"
cp "$GLUTEN_BUNDLE_JAR" "$SPARK_UNIFIED_LIB/gluten-velox-bundle.jar"
cp "$GLUTEN_BUNDLE_JAR" "$SPARK_V1_LIB/gluten-velox-bundle.jar"
ls -lh "$SPARK_UNIFIED_LIB" "$SPARK_V1_LIB"
echo "::endgroup::"

echo "::group::Patching DeltaSQLCommandTest to enable Gluten plugin"
TARGET="$DELTA_DIR/spark/src/test/scala/org/apache/spark/sql/delta/test/DeltaSQLCommandTest.scala"
if [ ! -f "$TARGET" ]; then
  echo "Expected file not found in Delta clone: $TARGET" >&2
  echo "The Delta directory layout for ref '${DELTA_REF}' may have changed."
  exit 1
fi
cp "$PATCH_SOURCE" "$TARGET"
echo "Patched $TARGET"
echo "--- diff vs. upstream ---"
git -C "$DELTA_DIR" --no-pager diff -- "spark/src/test/scala/org/apache/spark/sql/delta/test/DeltaSQLCommandTest.scala" || true
echo "::endgroup::"
