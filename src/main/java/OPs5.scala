object OPs5 {
  def main(args: Array[String]): Unit = {
    /**
      * 数组创建
      * 1、给定数组初始值
      * */
    val array1 = Array(1, 2, 3, 4);
    //2、固定长度的数组
    val array2 = new Array[Int](5); //java版： List<int> arrayList = new ArrayList<int>(); scala 使用[] 代替<>

    //数组遍历
    for(a <- array1) {
      print(" " + a);
    }
    println();
    for (index <- 0 until array1.length) {
      print(" " + array1(index));
    }
  }
}
