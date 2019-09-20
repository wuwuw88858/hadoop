import scala.collection.mutable._

object Ops6 {
  def main(args: Array[String]): Unit = {
    val list = List(10, 11, 12, 13);
    //列表添加元素 .:+ ==> 连接符
    val list1: List[Int] = list.:+(12);
    println(list1);

    //list之间连接 :::
    var l1 = List(1, 2, 3, 4, 5, 6);
    var l2 = List(7, 8, 9, 10, 11, 12);
    var l3: List[Int] = l1:::(l2);
    println(l3);
  }
}
