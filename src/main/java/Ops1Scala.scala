object Ops1Scala {
  def main(args: Array[String]): Unit = {
    val x = 1;
    val y = if (x > 0) 1 else -1;
    println(y);

    val z = if (x > 1) 1 else "error";  //支持混合类型表达式
    println(z);

    val m = if (x > 2) 1; //不大于的话，由于没有else,返回默认值：括号（）
    println(m);

    val k = if (x < 0) 0 else if (x >= 1) "k = 1"
    println(k)
  }
}
