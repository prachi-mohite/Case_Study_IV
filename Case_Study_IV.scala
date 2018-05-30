package Case_Study_IV

import Case_Studyy_III.Case_Study_III.Manual_schema_HVAC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Case_Study_IV {

  //case class for In Patient details
  case class InPatientCharges (DRGDefinition :String, ProviderId:Long, ProviderName:String, ProviderStreetAddress:String, ProviderCity:String,
    ProviderState:String, ProviderZipCode:Int, HospitalReferralRegionDescription:String, TotalDischarges:Int,
    AverageCoveredCharges:Double, AverageTotalPayments:Double, AverageMedicarePayments:Double)

  def main(args: Array[String]): Unit =
  {

    println("hey scala")
    //Create spark object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Hospital Case Study IV")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    println("Spark Session Object created")

    //Load the csv file into the RDD and create Dataframe from the same after mapping with case class
    val data = spark.sparkContext.textFile("E:\\Prachi IMP\\Hadoop\\Case Studies - Assignment\\Case Study IV\\inpatientCharges.csv")

    //Approach 2 With spark.read.format
    val data1 = spark.read.format("CSV")
      .option("header",true)
      .option("inferSchema",true)
      .load("E:\\Prachi IMP\\Hadoop\\Case Studies - Assignment\\Case Study IV\\inpatientCharges1.csv")

    data1.show()

    import spark.implicits._
    //Create Dataframe
    val header = data.first()
    println(header)
    val inPatientChargesDF = data.filter(row => row != header).map(_.split(",")).map(x=> InPatientCharges(DRGDefinition=x(0),ProviderId=x(1).toInt,
                            ProviderName = x(2),ProviderStreetAddress=x(3), ProviderCity=x(4),ProviderState=x(5),ProviderZipCode=x(6).toInt,
                            HospitalReferralRegionDescription=x(7),TotalDischarges=x(8).toInt,AverageCoveredCharges=x(9).toDouble,
                            AverageTotalPayments=x(10).toDouble,AverageMedicarePayments=x(11).toDouble)).toDF()

    //Task 1 : Loaded the data and displaying Rows of the Data
    inPatientChargesDF.show()

    //Task 2 : What is the average amount of AverageCoveredCharges per state
    //Approach 1 : Using SQL Spark Transformations
    inPatientChargesDF.groupBy("ProviderState").avg("AverageCoveredCharges").show()

    //Approach 2: Using the SQL Query
    inPatientChargesDF.createOrReplaceTempView("InPatientCharges_Details")
    spark.sql("Select ProviderState , avg(AverageCoveredCharges) from InPatientCharges_Details  group by ProviderState").show()

    //Task 3 : What is the average amount of AverageTotalPayments  per state
    //Approach 1 : Using SQL Spark Transformations
    inPatientChargesDF.groupBy("ProviderState").avg("AverageTotalPayments").show()

    //Approach 2: Using the SQL Query
     spark.sql("Select ProviderState , avg(AverageTotalPayments) from InPatientCharges_Details  group by ProviderState").show()

    //Task 4 : find out the AverageMedicarePayments charges per state.
    //Approach 1 : Using SQL Spark Transformations
    inPatientChargesDF.groupBy("ProviderState").avg("AverageMedicarePayments").show()

    //Approach 2: Using the SQL Query
     spark.sql("Select ProviderState , avg(AverageMedicarePayments) from InPatientCharges_Details  group by ProviderState").show()

    //Task 5 Find out the total number of Discharges per state and for each disease
    //Approach 1 : Using SQL Spark Transformations
    val task5Output= inPatientChargesDF.groupBy("ProviderState","DRGDefinition").sum("TotalDischarges").
      withColumnRenamed("sum(TotalDischarges)","sum")
    task5Output.show()

    //Approach 2 : Using SQL Query
    spark.sql("select ProviderState, DRGDefinition,sum(TotalDischarges) from InPatientCharges_Details group by ProviderState,DRGDefinition").show()

    //Task 6 Sort the output in descending order of totalDischarges
    //Approach 1: Using SQL Spark Transformations
    task5Output.sort(desc("sum")).show()
    //OR
    task5Output.orderBy(($"sum").desc).show()

    //Approach 2 : Using SQL Query
    task5Output.createOrReplaceTempView("task5Output_Table")
    spark.sql("select * from task5Output_Table order by sum desc").show()
  }
}
