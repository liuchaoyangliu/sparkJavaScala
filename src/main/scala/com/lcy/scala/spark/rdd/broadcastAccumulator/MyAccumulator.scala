package com.lcy.scala.spark.rdd.broadcastAccumulator

import org.apache.spark.util.AccumulatorV2

class MyAccumulator extends AccumulatorV2[String, String] {

    private var str = ""

    /**
     * 返回该累加器是否为零值
     *
     * @return
     */
    override def isZero: Boolean = str.equals("")

    /**
     * 创建此累加器的新副本
     *
     * @return
     */
    override def copy(): AccumulatorV2[String, String] = new MyAccumulator

    /**
     * 重置这个累加器，它是零值。 记必须调用‘isZero’
     */
    override def reset(): Unit = str = ""

    /**
     * 接受输入并累加
     *
     * @param v
     */
    override def add(v: String): Unit = str += v

    /**
     * 将另一个相同类型的累加器合并并加到这个累加器中并更新它的状态
     *
     * @param other
     */
    override def merge(other: AccumulatorV2[String, String]): Unit = str += other.value

    /**
     * 定义此累加器的当前值
     *
     * @return
     */
    override def value: String = str

}
