/*
import org.apache.spark.{SparkConf, SparkContext}
import Parser.{Review, extractAttributes, parseReview}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{when, _}

object Main {
  private val local_data_source = "./data/amazon.tsv"
  private val cluster_data_source = "hdfs://10.0.0.1:54310/data/amazon"

  // RDD
  /*def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("hyoztyur")
    sparkConf.setMaster("local[1]")

    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile(local_data_source)

    val reviews = rdd.filter(!_.startsWith("marketplace")).map(review => parseReview(review)).persist()
    val attributes = extractAttributes(reviews).persist()
    attributes.take(1).map(attribute => println(attribute.starRating.value, attribute.helpfulVotes.value, attribute.vineProgram.value, attribute.verifiedPurchase.value, attribute.body.value))
  }*/

  // DataFrame
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("hyoztyur")
    sparkConf.setMaster("local[1]")

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    // Review StructType (equals to scala case class)
    //    val reviewSchema = StructType(Array(
    //      StructField("star_rating", IntegerType, true),
    //      StructField("helpful_votes", StringType, true),
    //      StructField("vine", StringType, true),
    //      StructField("verified_purchase", StringType, true),
    //      StructField("review_body", StringType, true)
    //    ))

    // convert scala case class to StructType
    //    val schema = ScalaReflection.schemaFor[Review].dataType.asInstanceOf[StructType]
    //    val encoderSchema = Encoders.product[Review].schema
    //    encoderSchema.printTreeString()

    val df = sparkSession.read.option("sep", "\t").option("header", true).csv(local_data_source).persist()
    import sparkSession.implicits._

    // Feature Selection
    val columns = df.select("helpful_votes", "total_votes", "vine", "verified_purchase", "review_body")

    // Convert string data to categorical data
    // Reference 1
    val data = columns
      .withColumn("target", expr("case when helpful_votes / total_votes >= 0.75 then true else false end"))
//      .withColumn("star_rating", col("star_rating").cast("Integer")) // there are reviews with no star rating!
//      .withColumn("star_rating", when(col("star_rating") > 0,  col("star_rating").cast("Integer")).otherwise(0))
      .withColumn("helpful_votes", when(col("helpful_votes") > 2, true).otherwise(false))
      .withColumn("vine", when(col("vine") === "Y", true).otherwise(false))
      .withColumn("verified_purchase", when(col("verified_purchase") === "Y", true).otherwise(false))
      .withColumn("review_body", expr("case when size(split(review_body, ' ')) > 99 then 'Long'" + "when size(split(review_body, ' ')) > 4 then 'Medium' else 'Short' end"))
      .drop("total_votes")

    val attributes = Array("helpful_votes", "vine", "verified_purchase", "review_body")

    // reference 7
    val Array(training, test) = data.randomSplit(Array[Double](0.9, 0.1))
    training.persist()
    test.persist()

    val tree = decisionTree.DecisionTree.ID3(training, "target", attributes)
    println(tree)
    System.in.read()
  }
}
*/
