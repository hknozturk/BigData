/*
import org.apache.spark.rdd.RDD

object Parser {

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
  }
}*/
