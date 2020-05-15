/*
package decisionTree

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object DecisionTree {
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
}
*/
