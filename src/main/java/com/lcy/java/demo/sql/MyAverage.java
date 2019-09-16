package com.lcy.java.demo.sql;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.*;

import java.util.Arrays;

public class MyAverage extends UserDefinedAggregateFunction {

    // 数据类型的这个集合函数的输入参数
    @Override
    public StructType inputSchema() {

        return DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("input", DataTypes.DoubleType, true)));
    }

    // 聚合缓冲区中的值的数据类型
    @Override
    public StructType bufferSchema() {
        return DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("sum", DataTypes.DoubleType, true),
                        DataTypes.createStructField("count", DataTypes.LongType, true)
                ));
    }

    // 返回值的数据类型
    @Override
    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    // 此函数是否始终在相同的输入
    @Override
    public boolean deterministic() {
        return true;
    }

//    初始化给定的聚合缓冲区。缓冲区本身是一个“行”
//    提供了一些标准方法，比如在索引处检索值(例如get()、getBoolean())
//    更新其值的机会。注意，缓冲区内的数组和映射仍然是不可变的。
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, 0.0);
        buffer.update(1, 0L);
    }

//    使用来自“input”的新输入数据更新给定的聚合缓冲区“buffer”
//    局部累加
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        buffer.update(0, buffer.getDouble(0) + input.getDouble(0));
        buffer.update(1, buffer.getLong(1) + 1);
    }

//    合并两个聚合缓冲区并将更新后的缓冲区值存储回' buffer1 '
//    全局累加
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0));
        buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));
    }

//    计算最终结果
    @Override
    public Object evaluate(Row buffer) {
        return buffer.getDouble(0) / buffer.getLong(1);
    }
}
