package com.lcy.java.demo.rdd.broadcastAccumulator;


import org.apache.spark.util.AccumulatorV2;

import java.util.HashMap;
import java.util.Map;

public class UserDefinedAccumulator extends AccumulatorV2<String, String> {

    private String one = "ONE";
    private String two = "TWO";
    private String three = "THREE";
    //将想要计算的字符串拼接起来，并赋初始值，后续针对data进行累加，并返回
    private String data = one + ":0;" + two + ":0;" + three + ":0;";
    //原始状态
    private String zero = data;

    //判断是否是初始状态，直接与原始状态的字符串进行对比
    @Override
    public boolean isZero() {
        return data.equals(zero);
    }

    //复制一个新的累加器
    @Override
    public AccumulatorV2<String, String> copy() {
        return new UserDefinedAccumulator();
    }

    //重置，恢复原始状态
    @Override
    public void reset() {
        data = zero;
    }

    //针对传入的字符串，与当前累加器现有的值进行累加
    @Override
    public void add(String v) {
        data = mergeData(v, data, ";");
    }

    //将两个累加器的计算结果进行合并
    @Override
    public void merge(AccumulatorV2<String, String> other) {
        data = mergeData(other.value(), data, ";");
    }

    //将此累加器的计算值返回
    @Override
    public String value() {
        return data;
    }

    /**
     * 合并两个字符串
     * @param data_1 字符串1
     * @param data_2 字符串2
     * @param delimit 分隔符
     * @return 结果
     */
    private String mergeData(String data_1, String data_2, String delimit) {
        StringBuffer res = new StringBuffer();
        String[] infos_1 = data_1.split(delimit);
        String[] infos_2 = data_2.split(delimit);
        Map<String, Integer> map_1 = new HashMap<>();
        Map<String, Integer> map_2 = new HashMap<>();
        for (String info : infos_1) {
            String[] kv = info.split(":");
            if (kv.length == 2) {
                String k = kv[0].toUpperCase();
                Integer v = Integer.valueOf(kv[1]);
                map_1.put(k, v);
                continue;
            }
        }
        for (String info : infos_2) {
            String[] kv = info.split(":");
            if (kv.length == 2) {
                String k = kv[0].toUpperCase();
                Integer v = Integer.valueOf(kv[1]);
                map_2.put(k, v);
                continue;
            }
        }
        for (Map.Entry<String, Integer> entry : map_1.entrySet()) {
            String key = entry.getKey();
            Integer value = entry.getValue();
            if (map_2.containsKey(key)) {
                value = value + map_2.get(key);
                map_2.remove(key);
            }
            res.append(key + ":" + value + delimit);
        }
        for (Map.Entry<String, Integer> entry : map_1.entrySet()) {
            String key = entry.getKey();
            Integer value = entry.getValue();
            if (res.toString().contains(key)) {
                continue;
            }
            res.append(key + ":" + value + delimit);
        }
        if (!map_2.isEmpty()) {
            for (Map.Entry<String, Integer> entry : map_2.entrySet()) {
                String key = entry.getKey();
                Integer value = entry.getValue();
                res.append(key + ":" + value + delimit);
            }
        }
        return res.toString().substring(0, res.toString().length() - 1);
    }


}