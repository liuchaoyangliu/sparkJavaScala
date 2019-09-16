package com.lcy.scala.demo.rdd.secondarySort


class MySecondSort(val first:Int, val second: String)
  extends Ordered[MySecondSort] with Serializable{
  override def compare(that: MySecondSort): Int = {
    if(this.first - that.first != 0){
      this.first - that.first
    }  else {
      this.second.compareTo(that.second)
    }
  }

}