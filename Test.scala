/*
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("hyoztyur")
    sparkConf.setMaster("local[1]")

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    val df = sparkSession.read.option("sep", ",").option("header", true).csv("./data/test.csv").persist()

    val attributes = Array("Outlook", "Temperature", "Humidity", "Wind")

    val tree = decisionTree.DecisionTree.ID3(df, "play baseball", attributes)

    println(tree)

    System.in.read()


//    if (attr == "Wind") {
//      Array("Weak", "Strong")
//    } else if (attr == "Outlook") {
//      Array("Sunny", "Overcast", "Rain")
//    }else if (attr == "Temperature") {
//      Array("Hot", "Mild", "Cool")
//    }else if (attr == "Humidity") {
//      Array("High", "Normal")
//    }else {
//      Array(0, 1)
//    }
  }
}
*/
