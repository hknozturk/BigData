import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, expr, when}
import org.apache.spark.sql.functions._

object hyoztyur {
  private val local_data_source = "./data/amazon.tsv"
  private val cluster_data_source = "hdfs://10.0.0.1:54310/data/amazon"

  // DecisionTree
  type AttributeIdx = Int
  type Attribute = String
  type Dataset = DataFrame

  private final val THRESHOLD = 2

  def H(data: Dataset, target: Attribute, data_count: Long): Float = {
    val entropy = data.groupBy(target).count().withColumn("entropy", toCalculateEntropy(col("count"), lit(data_count)))
      // Reference 4
      // reduce(_ - _) returns wrong calculations time to time.
      .select("entropy").persist().rdd.map(_(0).asInstanceOf[Float]).reduce(_ + _)

    0 - entropy
  }

  def IG(data: Dataset, target: Attribute, A: Attribute, data_count: Long): Float = {
    // calculates for each attribute class (how to stop repeatedly calculated for same
    val dataset_entropy = H(data, target, data_count)

    // Calculate Information Gain
    var relativeEntropy: Float = 0
    // Reference 10
    // I know it is bad to use collect: it puts everything on driver. Couldn't find elegant solution.
    // data bring to driver is not huge, which will not crash the application.
    // FINALLY! I get rid of collect.
    // I tried below approach before, but I don't know why it didn't run H
    data.groupBy(A).count().rdd.map(row => {
      val p = row(1).toString.toFloat / data_count
      val entropy = H(data.filter(col(A) === row(0).toString), target, row(1).toString.toLong)
      val pxEntropy = p * entropy

      relativeEntropy += pxEntropy
    })

    /* data.groupBy(A).count().collect().foreach(row => {
       val p = row(1).toString.toFloat / data_count
       val entropy = H(data.filter(col(A) === row(0).toString), target, row(1).toString.toLong)
       val pxEntropy = p * entropy

       relativeEntropy += pxEntropy
     })*/

    //    data.groupBy(A).count().repartition(col(A)).persist().foreachPartition(rows => {
    //      if (rows.nonEmpty) {
    //        rows.foreach(row => {
    //          val p = row(1).toString.toFloat / data_count
    //          val entropy = H(d.filter(col(A) === row(0).toString), target, row(1).toString.toLong)
    //          val pxEntropy = p * entropy
    //
    //          relativeEntropy += pxEntropy
    //        })
    //      }
    //    })
    dataset_entropy - relativeEntropy
  }

  def ID3(data: Dataset, target: Attribute, attributes: Array[Attribute]): DecisionTree = {
    // If one of three conditions hold, stop recursion:
    //    1- The set of examples that need to be classified is smaller than some threshold (prevents over-fitting)
    //    2- The list of predictive attributes is empty
    //    3- All examples in data have the same value for the target attribute (zero entropy)
    // Return a single-node tree with as a label the most common value of the target attribute.
    val data_count = data.count()

    if (data_count < THRESHOLD || attributes.isEmpty || H(data, target, data_count) == 0) {
      leafTree(mostCommonValue(data, target))
    } else {
      // Find the attribute that best classifies the data (maximum information gain)
      //      val start = System.nanoTime()
      val ig = attributes.map(attribute => (attribute, IG(data, target, attribute, data_count)))
      //      val stop = System.nanoTime()
      //      println("IG execution time: " + (stop - start))
      //      val start1 = System.nanoTime()
      // This step takes approximately same amount of time as calculating ig. It double the execution time of the application
      // Check Notes.md (Appendix: Table 1)
      //      val A = attributes.find(IG(data, target, _, data_count) == ig.max).get
      val A = ig.maxBy(_._2)._1
      //      val stop1 = System.nanoTime()
      //      println("A execution time: " + (stop1 - start1))

      // Construct a new decision tree with root attribute A
      val tree = decisionTree(A)

      // For every possible class/value v of A, add a new branch with a subtree that classifies
      // all datapoints in data for which attribute A has value v.
      for (v <- possibleValues(A)) {
        // Reference 9
        // according to this article, when we filter data to smaller ones, we should almost always repartition it.
        // In my local machine execution of repartitioned one was longer.
        val filtered = data.filter(col(A) === v)/*.repartition(col(A))*/.persist()
        val subtree = ID3(filtered, target, attributes.filter(_ != A))
        tree.addSubTree(v, subtree)
      }

      tree
    }
  }

  abstract class DecisionTree {
    def getValues(): Unit
  }
  case class leafTree(attributeClass: Any) extends DecisionTree {
    val attrClass = attributeClass

    def getValues() = {
      println("leaf tree outcome " + attrClass)
    }
  }

  case class decisionTree(attribute: Attribute) extends DecisionTree {
    var subTree: Array[Tree] = Array()
    def addSubTree(attributeClass: Any, decisionTree: DecisionTree): Unit = {
      subTree :+= Tree(decisionTree, attributeClass)
    }

    def getValues() = {
      println("subtrees " + subTree)
    }
  }

  case class Tree(tree: DecisionTree, attributeClass: Any)

  def mostCommonValue(dataset: Dataset, target: Attribute): Any = {
    // isEmpty is an action operator !!! Change it later.
    if (dataset.isEmpty) {
      false
    } else {
      dataset.groupBy(target).count().orderBy(desc("count")).select(target).first().get(0)
    }
  }

  // TO-DO: implement quartile classification for the body length instead of hard-coded classification.
  def possibleValues(attr: Attribute): Array[Any] = {
    if (attr == "star_rating") {
      Array(0, 1, 2, 3, 4, 5)
    } else if (attr == "review_body") {
      Array("Short", "Medium", "Long")
    } else {
      Array(true, false)
    }
  }

  // Reference 2, 3
  def calculateEntropy(count: Long, total: Long): Option[Float] = {
    val cnt = Option(count).getOrElse(return None)
    val p = cnt.toFloat / total

    Some(p * (math.log(p) / math.log(2)).toFloat)
  }

  val toCalculateEntropy = udf(calculateEntropy _)

  // Prediction
  /*
    Reference 8
    For predicting a input data class label (true or false);
    - Start from root of the tree
    - Compare values of root tree with input attribute
    - Follow the branch matches the value
    - Move down to subTree
    - Repeat until reach to leafTree with the predicted class value
     */
  def predict(input: Row, attributes: Array[String], tree: DecisionTree): Boolean = {
    var t = tree
    var correct = 0
    var total = 0
    while(t.isInstanceOf[decisionTree]) {
      val node:decisionTree = t.asInstanceOf[decisionTree]
      val value = input.get(attributes.indexOf(node.attribute))
      t = node.subTree.filter(_.attributeClass == value)(0).tree
    }

    val l = t.asInstanceOf[leafTree].attrClass.toString.toBoolean
    // match with the target attr (true - false).
    l
  }

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
    val columns = df.select("star_rating", "helpful_votes", "total_votes", "vine", "verified_purchase", "review_body")

    // Convert string data to categorical data
    // Reference 1
    val data = columns
      .withColumn("target", expr("case when helpful_votes / total_votes >= 0.75 then true else false end"))
      //      .withColumn("star_rating", col("star_rating").cast("Integer")) // there are reviews with no star rating!
      .withColumn("star_rating", when(col("star_rating") > 0,  col("star_rating").cast("Integer")).otherwise(0))
      .withColumn("helpful_votes", when(col("helpful_votes") > 2, true).otherwise(false))
      .withColumn("vine", when(col("vine") === "Y", true).otherwise(false))
      .withColumn("verified_purchase", when(col("verified_purchase") === "Y", true).otherwise(false))
      .withColumn("review_body", expr("case when size(split(review_body, ' ')) > 99 then 'Long'" + "when size(split(review_body, ' ')) > 4 then 'Medium' else 'Short' end"))
      .drop("total_votes")

    val attributes = Array("star_rating", "helpful_votes", "vine", "verified_purchase", "review_body")

    // reference 7
    val Array(training, test) = data.randomSplit(Array[Double](0.9, 0.1))
    training.persist()
    test.persist()

    val tree = ID3(training, "target", attributes)
    println(tree)
    System.in.read()
  }
}

// RDD - couldn't finish
// PARSER
/*
abstract class Attribute[T]{
  def possibleValues(): Array[T]
}

class Rating(val n: Int) extends Attribute[Int] with Serializable {
  def value = n
  def possibleValues() = Array(1, 2, 3, 4, 5)
}

class YesNo(val n: String) extends Attribute[String] with Serializable {
  def value = if (n == "Y") "Yes" else "No"
  def possibleValues() = Array("Yes", "No")
}

class Helpful(val n: Int) extends Attribute[String] with Serializable {
  def value = if (n > 4) "Yes" else "No"
  def possibleValues() = Array("Yes", "No")
}

class LikerLength(val n: String) extends Attribute[String] with Serializable {
  val bodyLength = n.length
  def value = if (bodyLength < 4) "Short" else if (bodyLength < 101) "Medium" else "Long"
  def possibleValues() = Array("Short", "Medium", "Long")
}

case class Review(
                   starRating: String,
                   helpfulVotes: String,
                   vine: String,
                   verifiedPurchase: String,
                   body: String
                 )

case class Attributes(
                       starRating: Rating,
                       helpfulVotes: Helpful,
                       vineProgram: YesNo,
                       verifiedPurchase: YesNo,
                       body: LikerLength
                     )

def parseReview(review: String): Review = {
  val params = review.split("\t")
  val newReview = Review(params(7), params(8), params(10), params(11), params(13))
  newReview
}

def extractAttributes(reviews: RDD[Review]): RDD[Attributes] = {
  reviews.map(review => Attributes(
    new Rating(review.starRating.toInt),
    new Helpful(review.helpfulVotes.toInt),
    new YesNo(review.vine),
    new YesNo(review.verifiedPurchase),
    new LikerLength(review.body)
  ))
}*/

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
