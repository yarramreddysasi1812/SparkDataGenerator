package in.pramati.mocker

/**
 * Created by Sasidhar().
 *
 */
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.io.{File, FileReader}
import com.google.gson.JsonParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, _}
import java.io.{File, FileReader}

import in.pramati.mocker.DataFrameUtil._


import scala.util.{Random, Success, Try}


object BatchJob {

  val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\Softwares\\hadoop-2.7.2_winbinariesx64")
    implicit val spark = getSparkSession

    val applicationId: String = spark.sparkContext.applicationId
  //  val idf=spark.read.parquet("C://Users//TDT//Desktop//part-00194-216bf9f6-47c2-4857-8cad-3232c7982448.c000.gz.parquet")
  //idf.printSchema()


    /*    val rcount = args(0).trim
        val configPath = args(1).trim
        val outputPath = args(2).trim*/

    val numberOfRowsToBeGenerated = 10
    val numberOfPartFilesToBeGenerated=2
    val positiveRecordpercent =0.75
    val intArray = (1 to 999)
   // val longArray = (1L to 999L)
    val rangelist = intArray.mkString(":")
    val myRandomStringArray = Array("Long Live     " + rangelist
      , "Long Live Testing 1 " + rangelist
      , "Long Live Testing 2 " + rangelist, null)
  //  val configPath = "C://Users//TDT//IdeaProjects//SampleData//src//test//resources//"
  //  val outputPath = args(2).trim

    import com.google.gson.stream.JsonReader

    val reader = new JsonReader(new FileReader("C://Users//TDT//Desktop//MDG//e.json"))//avaialable in src/resources
    val o = new JsonParser().parse(reader).getAsJsonObject
    val myjsonasString = o.getAsJsonArray("columns").toString
    import spark.implicits._
    val dataFrameFromJson = spark.read.json(Seq(myjsonasString).toDS())
    dataFrameFromJson.printSchema()

    val dataFrameOfRequiredColumns = dataFrameFromJson.selectExpr("businessColumnName", "orderOfColumnToBeLoadedInToDatabase", "type", "length")
    dataFrameOfRequiredColumns.show()
    val keyedBy = dataFrameOfRequiredColumns.rdd.keyBy(_.getAs[Long]("orderOfColumnToBeLoadedInToDatabase"))

    keyedBy.foreach(x => println("using keyBy-->>" + x))
    val mySortedMapByColOrder = scala.collection.immutable.TreeMap(keyedBy.collectAsMap().toArray: _*)
    val list = scala.collection.mutable.ListBuffer.empty[StructField]
    for ((key, value) <- mySortedMapByColOrder) {
      list += StructField(value.get(0).toString,
        createStruct(value.get(2).toString), false)
    }
    logger.debug("list is " + list.toList)
    val schema = StructType(list.toList)
    logger.info("schema as treeString -----" + schema.treeString)

    //val replicatedDf1 = replicateDf(100, testDf)// logger.info("-----" + schema.treeString)// creating data from sample values
    val mytestdataList = scala.collection.mutable.ListBuffer.empty[Any]
    //val schema1 = list1.toList
    for ((key1, value1) <- mySortedMapByColOrder) {
    logger.info(value1)
    val value = if (value1.get(2) == null) "" else value1
    logger.info(value)
    logger.info("value1.getAs[Long](\"length\")" + value1.getAs[Long]("length"))
      val valueOfLength: Int = Try(value1.getAs[Long]("length")) match {
        case Success(s) => s.toString.toInt
        case _ => logger.info("error :value1.getAs[Long](\"length\") is null"); 0

      }
      mytestdataList += createTestData(value.toString, valueOfLength)
    }
    var row = Row.fromSeq(mytestdataList.toSeq)
    row = Row.fromSeq(mytestdataList.toSeq)
    logger.info("list is " + mytestdataList)
    val testdatadf: RDD[Row] = spark.sparkContext.parallelize(Seq(row))
    logger.info("printing each row ")
    testdatadf.foreach(logger.info)
    val testdf: DataFrame = spark.createDataFrame(testdatadf, schema)
    logger.info("row is " + row.getClass.getName + "-----> " + row)
    testdf.show
    testdf.printSchema()
    logger.info(":::::::Experiment to replicate data!!!")
    val dfReplicated = replicateOneRowManytimes(numberOfRowsToBeGenerated, testdf)
    //val df2 = arrayOfReplicated(1).withColumn("","");
    val ided = addColumnIndex(dfReplicated, "index")

    moveColumnToFirstPos(ided, "index")
    val (training, test1) = splitDF(dfReplicated, positiveRecordpercent, 1 - positiveRecordpercent)
    val mycols = test1.schema.fields
    var test = test1;
    mycols.foreach(x => {
      test =
        x.dataType match {
          case FloatType => test.withColumn(x.name, lit(Random.nextFloat()))
          case DoubleType => test.withColumn(x.name, lit(Random.nextDouble()))
          case IntegerType => test.withColumn(x.name, lit(intArray(Random.nextInt(intArray.size))))
          case StringType => test.withColumn(x.name, lit(myRandomStringArray(Random.nextInt(myRandomStringArray.size))))
          case LongType => test.withColumn(x.name, lit(Random.nextLong()))
          // any data types you want to add thats upto your data requirements//case TimestampType => test //.withColumn(x.name, Timestamp.valueOf("2017-12-02 03:04:00"))case _ => test
        }

    })
    val finaldf = training.union(test)
    logger.info(finaldf.count())
    test.show(false)
    finaldf.printSchema()
    finaldf.show(false)

    val dfToWrite = finaldf.coalesce(numberOfPartFilesToBeGenerated).write.mode(SaveMode.Overwrite)
    val allFormats = Array("PARQUET", "AVRO", "CSV", "JSON", "XML")
    val  format="ALL"
    format match {

      case "ALL" =>
        logger.info(s"Writing all formats ${allFormats.mkString("\n")} ");
        allFormats.foreach(formatType => writeToFileFormats(formatType, dfToWrite));
      case _ =>
        logger.info(s"Writing $format format } ");
        writeToFileFormats(format, dfToWrite)
    }
  }
    def getSparkSession(): SparkSession = {
      val conf: SparkConf = new SparkConf()
        .setAppName("MockDataCreation")


        logger.info("Starting Spark Session.")
    implicit val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.master", "local")
/*      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")*/
      .config("spark.driver.maxResultSize","0")
      .getOrCreate()
    spark
  }
}
