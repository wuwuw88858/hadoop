object Ops4 {
  //方法定义
//  def m1(a: Int, b: String): String = { //Unit 相当于void 无返回值
//    println("hello");
//     a + " " + b;  //相当于 return a + " " + b;
//  }
//
//  def main(args: Array[String]): Unit = {
//    println(m1(1, "name"));
//  }

  //函数定义
//  def main(args: Array[String]): Unit = {
//    val f1 = (a: Int, b: Int) => a + b
//    println(f1(1, 3));
//  }

  def main(args: Array[String]): Unit = {
    //匿名函数(没有名称的函数， 单独定义函数的时候，必须声明形参的类型[x: Int, y: Int]）
    val f1 = (x: Int, y: Int) => x + y;

  }
}
