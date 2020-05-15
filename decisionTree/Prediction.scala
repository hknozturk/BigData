/*
package decisionTree

import org.apache.spark.sql.Row

object Prediction {
  /*
    Reference 8
    For predicting a input data class label (true or false);
    - Start from root of the tree
    - Compare values of root tree with input attribute
    - Follow the branch matches the value
    - Move down to subTree
    - Repeat until reach to leafTree with the predicted class value
     */
  def predict(input: Row, attributes: Array[String], tree: DecisionTree.DecisionTree): Boolean = {
    var t = tree
    var correct = 0
    var total = 0
    while(t.isInstanceOf[DecisionTree.decisionTree]) {
      val node:decisionTree.DecisionTree.decisionTree = t.asInstanceOf[DecisionTree.decisionTree]
      val value = input.get(attributes.indexOf(node.attribute))
      t = node.subTree.filter(_.attributeClass == value)(0).tree
    }

    val l = t.asInstanceOf[DecisionTree.leafTree].attrClass.toString.toBoolean
    // match with the target attr (true - false).
    l
  }
}
*/
