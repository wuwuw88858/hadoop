object Ops3 {
  def main(args: Array[String]): Unit = {
//    for (a <- 1 to 10) {
//      for(b <- 1 to 10) {
//        if (a == b) {
//          println(a + " " + b);
//        }
//      }
//    }
    //for循环新式写法
//    for (a <- 1 to 10; b <- 1 to 10 if a == b) {
//      println(a + " " + b);
//    }

    //定义数组
//    val array = Array[Int](1, 2, 3, 4, 5);
//    val array1 = new Array[Int](array.length);
//    for (index <- 0 until  array.length) {  //这里需要until 用 to 会 数组长度溢出
//      array1(index) = array(index) + 10;
//    }
//    for (a <- array1) {
//      print (a + " ");
//    }

    //yield 不能出现循环体 ： 遍历array数组，然后执行yield后面的表达式（a+ 10) 在给到ret
    val array = Array[Int](1, 2, 3, 4, 5);
    var ret = for (a <- array) yield a + 10;
    println(ret.toBuffer);
  }
}
