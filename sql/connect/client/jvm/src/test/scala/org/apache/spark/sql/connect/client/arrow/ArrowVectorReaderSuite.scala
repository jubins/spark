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
package org.apache.spark.sql.connect.client.arrow

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.TimeStampMicroTZVector

import org.apache.spark.sql.connect.test.ConnectFunSuite
import org.apache.spark.sql.types.{TimestampLTZNanosType, TimestampNTZNanosType, TimestampType}
import org.apache.spark.sql.util.ArrowUtils

class ArrowVectorReaderSuite extends ConnectFunSuite {

  private val allocator = new RootAllocator()

  override def afterAll(): Unit = {
    allocator.close()
    super.afterAll()
  }

  // Build a TimeStampMicroTZVector (the Arrow encoding for TimestampType) backed by a live
  // allocator. This is the vector a Connect server would send for any LTZ timestamp column.
  private def microTZVector(): TimeStampMicroTZVector = {
    val field = ArrowUtils.toArrowField("ts", TimestampType, nullable = true, "UTC")
    field.createVector(allocator).asInstanceOf[TimeStampMicroTZVector]
  }

  test("SPARK-57738: ArrowVectorReader rejects TimestampLTZNanosType with a clear error") {
    val vector = microTZVector()
    try {
      val ex = intercept[RuntimeException] {
        ArrowVectorReader(TimestampLTZNanosType(9), vector, "UTC")
      }
      assert(
        ex.getMessage.contains("not yet supported"),
        s"Expected 'not yet supported' in error message, got: ${ex.getMessage}")
    } finally {
      vector.close()
    }
  }

  test("SPARK-57738: ArrowVectorReader rejects TimestampNTZNanosType with a clear error") {
    val vector = microTZVector()
    try {
      val ex = intercept[RuntimeException] {
        ArrowVectorReader(TimestampNTZNanosType(7), vector, "UTC")
      }
      assert(
        ex.getMessage.contains("not yet supported"),
        s"Expected 'not yet supported' in error message, got: ${ex.getMessage}")
    } finally {
      vector.close()
    }
  }

  test("SPARK-57738: ArrowVectorReader still succeeds for plain TimestampType") {
    val vector = microTZVector()
    try {
      val reader = ArrowVectorReader(TimestampType, vector, "UTC")
      assert(reader != null)
    } finally {
      vector.close()
    }
  }
}
