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

package org.apache.spark.sql.jdbc.esg

import org.apache.spark.Partition
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCPartition, JDBCPartitioningInfo, JDBCRelation}
import org.apache.spark.sql.jdbc.esg.JDBCUtils._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}

import scala.collection.mutable.ArrayBuffer

class DefaultSource extends CreatableRelationProvider with RelationProvider with DataSourceRegister {

  override def shortName(): String = "jdbc"

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val partitionColumn = jdbcOptions.partitionColumn
    //分区字段
    val lowerBound = jdbcOptions.lowerBound
    val upperBound = jdbcOptions.upperBound
    val numPartitions = jdbcOptions.numPartitions

    val partitionInfo = if (partitionColumn == null) {
      //TODO 新加高版本的安全校验
      assert(lowerBound == null && upperBound == null, "When 'partitionColumn' is not specified.")
      null
    } else {
      //TODO 新加高版本的安全校验
      assert(lowerBound.nonEmpty && upperBound.nonEmpty && numPartitions.nonEmpty, s"When 'partitionColumn' is specified.")
      JDBCPartitioningInfo(partitionColumn.toString, IntegerType, lowerBound.toString.toLong, upperBound.toString.toLong, numPartitions.toString.toInt)
    }
    val parts = columnPartition(partitionInfo)
    JDBCRelation(parts, jdbcOptions)(sqlContext.sparkSession)
  }
  def columnPartition(partitioning: JDBCPartitioningInfo): Array[Partition] = {
    if (partitioning == null || partitioning.numPartitions <= 1 ||
      partitioning.lowerBound == partitioning.upperBound) {
      return Array[Partition](JDBCPartition(null, 0))
    }

    val lowerBound = partitioning.lowerBound
    val upperBound = partitioning.upperBound
    require (lowerBound <= upperBound,
      "Operation not allowed: the lower bound of partitioning column is larger than the upper " +
        s"bound. Lower bound: $lowerBound; Upper bound: $upperBound")

    val numPartitions =
      if ((upperBound - lowerBound) >= partitioning.numPartitions) {
        partitioning.numPartitions
      } else {
        upperBound - lowerBound
      }
    // Overflow and silliness can happen if you subtract then divide.
    // Here we get a little roundoff, but that's (hopefully) OK.
    val stride: Long = upperBound / numPartitions - lowerBound / numPartitions
    val column = partitioning.column
    var i: Int = 0
    var currentValue: Long = lowerBound
    var ans = new ArrayBuffer[Partition]()
    while (i < numPartitions) {
      val lBound = if (i != 0) s"$column >= $currentValue" else null
      currentValue += stride
      val uBound = if (i != numPartitions - 1) s"$column < $currentValue" else null
      val whereClause =
        if (uBound == null) {
          lBound
        } else if (lBound == null) {
          s"$uBound or $column is null"
        } else {
          s"$lBound AND $uBound"
        }
      ans += JDBCPartition(whereClause, i)
      i = i + 1
    }
    ans.toArray
  }
  //TODO parameters需要携带url,table
  //TODO createTableOptions  : ENGINE=InnoDB DEFAULT CHARSET=utf8
  //TODO isTruncate : if to truncate the table from the JDBC database
  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               df: DataFrame): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val url = jdbcOptions.url
    val table = jdbcOptions.parameters(JDBCOptions.JDBC_TABLE_NAME)
    val createTableOptions = jdbcOptions.createTableOptions
    val isTruncate = jdbcOptions.isTruncate

    //todo 自适配update
    var saveMode = mode match {
      case SaveMode.Append => CustomSaveMode.Append
      case SaveMode.Overwrite => CustomSaveMode.Overwrite
      case SaveMode.ErrorIfExists => CustomSaveMode.ErrorIfExists
      case SaveMode.Ignore => CustomSaveMode.Ignore
    }
    val parameterLower = parameters.map(line => (line._1.toLowerCase(), line._2))
    if (parameterLower.keySet.contains("savemode")) {
      println(s"########${parameterLower("savemode")}#########")
      saveMode = if (parameterLower("savemode").equals("update")) CustomSaveMode.Update else saveMode
    }

    val conn = JDBCUtils.createConnectionFactory(jdbcOptions)()
    try {
      val tableExists = JDBCUtils.tableExists(conn, url, table)
      //提前去数据库拿表的schema信息
      val tableSchema = JDBCUtils.getSchemaOption(conn, jdbcOptions)

      if (tableExists) {
        saveMode match {
          case CustomSaveMode.Overwrite =>
            if (isTruncate && isCascadingTruncateTable(url) == Some(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, table)
              saveTable(df, url, table, tableSchema, saveMode, jdbcOptions)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, table)
              createTable(df.schema, url, table, createTableOptions, conn)
              saveTable(df, url, table, Some(df.schema), saveMode, jdbcOptions)
            }

          case CustomSaveMode.Append =>
            saveTable(df, url, table, tableSchema, saveMode, jdbcOptions)

          case CustomSaveMode.ErrorIfExists =>
            throw new AnalysisException(
              s"Table or view '$table' already exists. SaveMode: ErrorIfExists.")

          case CustomSaveMode.Ignore =>
          // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
          // to not save the contents of the DataFrame and to not change the existing data.
          // Therefore, it is okay to do nothing here and then just return the relation below.
          case CustomSaveMode.Update =>
            saveTable(df, url, table, tableSchema, saveMode, jdbcOptions)
        }
      } else {
        createTable(df.schema, url, table, createTableOptions, conn)
        saveTable(df, url, table, Some(df.schema), saveMode, jdbcOptions)
      }
    } finally {
      conn.close()
    }

    createRelation(sqlContext, parameters)
  }
}
