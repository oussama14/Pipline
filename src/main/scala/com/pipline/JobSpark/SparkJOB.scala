package com.pipline.JobSpark
import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}


object UseMail {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Statistics")
      .getOrCreate()
    import spark.implicits._

    val path_csv  = "/home/oussama/Bureau/SparkTestTechniques/film.csv"
    val data = spark.read
      .format("csv")
      .option("delimiter",";")
      .option("header","true")
      .option("inferSchema","true")
      .load(path_csv)
    val  columns = data.columns.toList.map(_.replace(" ","").replace("*",""))
    val df = data.toDF(columns :_*)
    df.printSchema()
    val df1 = df.filter(df ("Length")  !== "INT")

    val structureSchema = new StructType()
      .add(columns(0)+"___",IntegerType)
      .add(columns(1)+"___",IntegerType)
      .add(columns(2),StringType)
      .add(columns(3),StringType)
      .add(columns(4),StringType)
      .add(columns(5),StringType)
      .add(columns(6),StringType)
      .add(columns(7),IntegerType)
      .add(columns(8),StringType)
      .add(columns(8),StringType)
    df1.write.csv("/home/oussama/Bureau/SparkTestTechniques/film3.csv")
  }


}

