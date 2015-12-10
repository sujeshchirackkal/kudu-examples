package org.kududb.examples.sample

import scala.collection.mutable.ListBuffer
import scala.compat.Platform

import scala.collection.JavaConverters._

import org.kududb.{ ColumnSchema, Schema, Type }
import org.kududb.client._

object Sample extends App {

  // by default the code connects to the quick start VM
  val KUDU_MASTER: String = "quickstart.cloudera"
  println(s"Will try to connect to Kudu master at ${KUDU_MASTER}")

  val kuduClient: KuduClient = new KuduClient.KuduClientBuilder(KUDU_MASTER).build()

  val tableName = s"scala_sample_${Platform.currentTime}"

  try {
    // define the columns, and hence the schema
    val keyColumn = new ColumnSchema
      .ColumnSchemaBuilder("key", Type.INT32)
      .key(true)
      .build()

    val valueColumn = new ColumnSchema
      .ColumnSchemaBuilder("value", Type.STRING)
      .build()

    val columns = new ListBuffer[ColumnSchema]
      columns ++= Seq(keyColumn, valueColumn)

    val schema: Schema = new Schema(columns.asJava)
      kuduClient.createTable(tableName, schema)

    val table: KuduTable = kuduClient.openTable(tableName)
    val kuduSession: KuduSession = kuduClient.newSession()

    // insert a few key value pairs into the table
    1 to 10 foreach { i =>
      val insert: Insert  = table.newInsert()
      val row: PartialRow = insert.getRow()
      row.addInt(0, i)
      row.addString(1, s"value${i}")
      kuduSession.apply(insert)
    }

    println(s"inserted records successfully into table ${tableName}")

    // read the table and print the values
    val projectColumns = new ListBuffer[String]
    projectColumns += "value"

    val kuduScanner: KuduScanner = kuduClient
      .newScannerBuilder(table)
      .setProjectedColumnNames(projectColumns.asJava)
      .build()

    while (kuduScanner.hasMoreRows) {
      val resultIterator: RowResultIterator = kuduScanner.nextRows()
      while (resultIterator.hasNext) {
        val result: RowResult = resultIterator.next()
        println(s"Value from scan ${result.getString(0)}")
      }
    }

    println(s"scanning completed successfully from table ${tableName}")
  } catch {
    case e: Exception => {
      e.printStackTrace()
    }
  }
  finally {
    kuduClient.shutdown()
  }
}
