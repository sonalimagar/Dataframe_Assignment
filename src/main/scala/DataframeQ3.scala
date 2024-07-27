import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DataframeQ3 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","SPark-program")
    sparkconf.set("spark.master","local[*]")

    val spark =SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val transactions = List(
      (1, 1000),
      (2, 200),
      (3, 5000),
      (4,700)
    ).toDF("transaction_id", "amount")


    transactions.withColumn("category",when(col("amount")>1000 ,"High")
      .when(col("amount")>500 && col("amount")<1000,"Medium")
      .otherwise("low")).show()
  }

}
