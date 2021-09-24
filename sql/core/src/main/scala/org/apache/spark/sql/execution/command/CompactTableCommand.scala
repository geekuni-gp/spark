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
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType

case class CompactTableCommand(table: TableIdentifier,
                               fileNum: Option[Int]) extends LeafRunnableCommand {
  private val tmpTable: TableIdentifier = TableIdentifier(table.identifier + "_tmp")

  private val defaultSize = 128 * 1024 * 1024

  override def output: Seq[Attribute] = Seq(
    AttributeReference("compact_table_stmt", StringType, nullable = false)()
  )

  override def run(spark: SparkSession): Seq[Row] = {
    spark.catalog.setCurrentDatabase(table.database.getOrElse("default"))

    val tmpDF = spark.table(table.identifier)
    var partitions = fileNum match {
      case Some(num) => num
      case None => (spark.sessionState
        .executePlan(tmpDF.queryExecution.logical)
        .optimizedPlan.stats.sizeInBytes / defaultSize).toInt
    }

    if (partitions <= 0) partitions = 1

    tmpDF.repartition(partitions)
      .write.mode(SaveMode.Overwrite)
      .saveAsTable(tmpTable.identifier)

    spark.table(tmpTable.identifier)
      .write.mode(SaveMode.Overwrite)
      .saveAsTable(table.identifier)

    ;// 清除临时表
    spark.sql(s"DROP TABLE ${tmpTable.identifier}")
    Seq(Row(s"Compact Table $table Completed"))
  }
}
