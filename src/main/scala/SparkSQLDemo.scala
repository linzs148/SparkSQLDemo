import com.alibaba.druid.sql.ast.SQLStatement
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.io.Source
import scala.sys.exit

object SparkSQLDemo {

    val structTypeMap: mutable.Map[String, StructType] = mutable.Map()

    def loadSchama(): Unit = {
        // 初始化各个表的结构
        val lines: Iterator[String] = Source.fromFile(this.getClass.getResource("tpcds.sql").getPath, "UTF-8").getLines()
        val createTableSQL: mutable.StringBuilder = new mutable.StringBuilder
        lines.foreach(line => {
            // 将tpcds.sql文件按照`;`进行分割
            if (line.contains(";")) {
                val idx: Int = line.indexOf(';')
                createTableSQL.append(line.substring(0, idx))
                // 利用druid解析建表语句
                val parser: MySqlStatementParser = new MySqlStatementParser(createTableSQL.toString)
                val statement: SQLStatement = parser.parseStatement
                var structFieldList: List[StructField] = List()
                if (statement.isInstanceOf[MySqlCreateTableStatement]) {
                    val createStatement = statement.asInstanceOf[MySqlCreateTableStatement]
                    createStatement.getTableElementList.forEach(field => {
                        // 提取出建表语句的每个字段
                        if (field.isInstanceOf[SQLColumnDefinition]) {
                            val column: SQLColumnDefinition = field.asInstanceOf[SQLColumnDefinition]
                            val fieldName: String = column.getName.getSimpleName
                            val dataType: String = column.getDataType.getName
                            val nullable: Boolean = !column.containsNotNullConstaint
                            structFieldList :+= StructField(fieldName, transform(dataType), nullable)
                        }
                    })
                    val structType: StructType = StructType(structFieldList)
                    // 将表的名称和结构进行映射
                    structTypeMap += (createStatement.getName.getSimpleName -> structType)
                }
                createTableSQL.clear()
                createTableSQL.append(line.substring(idx + 1))
            } else {
                createTableSQL.append(line)
            }
        })
    }

    def transform(dataType: String): DataType = dataType match {
        // MySQL数据类型和Spark数据类型转换
        case "integer" => IntegerType
        case "decimal" => DoubleType
        case "char" => StringType
        case "varchar" => StringType
        case "date" => DateType
        case "time" => DateType
    }

    def readQuerySQL(sqlPath: String): String = {
        // 从指定文件读取sql查询语句
        val lines: Iterator[String] = Source.fromFile(sqlPath, "UTF-8").getLines()
        val stringBuilder: mutable.StringBuilder = new mutable.StringBuilder
        lines.foreach(line => stringBuilder.append(line))
        stringBuilder.toString
    }

    def loadData(sparkSession: SparkSession, dataDir: String, querySQL: String, parallelism: Int): Unit = {
        // 对查询sql中涉及到的表进行数据加载
        structTypeMap.keys.foreach(tableName => {
            if (querySQL.contains(tableName)) {
                val structType: StructType = structTypeMap(tableName)
                // 根据传入的并行度进行分区
                val dataFrame: DataFrame = sparkSession.createDataFrame(sparkSession.sqlContext.read.option("sep", "|").schema(structType).csv(dataDir + "/" + tableName + ".dat").repartition(parallelism).rdd, structType)
                // 立即计算
                dataFrame.count()
                dataFrame.createOrReplaceTempView(tableName)
            }
        })
    }

    def main(args: Array[String]): Unit = {
        if (args.length != 4) exit(-1)
        val dataDir: String = args.apply(0)
        val sqlPath: String = args.apply(1)
        val outputDir: String = args.apply(2)
        val parallelism: Int = args.apply(3).toInt
        val sparkSession: SparkSession = SparkSession.builder()
          .master("local[*]")
          .appName("SparkSQLDemo")
          .getOrCreate()
        // 读取原数据和数据
        loadSchama()
        val querySQL: String = readQuerySQL(sqlPath)
        loadData(sparkSession, dataDir, querySQL, parallelism)
        // 执行查询sql
        val startTime = System.currentTimeMillis()
        val dataFrame: DataFrame = sparkSession.sqlContext.sql(querySQL)
        // 立即计算
        dataFrame.count()
        val endTime = System.currentTimeMillis()
        println("SQL执行时间: " + (endTime - startTime) + "ms")
        // 查询结果持久化到文件
        dataFrame.coalesce(1).write.mode("append").format("csv").option("sep", "|").option("header", "true").save(outputDir)
    }
}