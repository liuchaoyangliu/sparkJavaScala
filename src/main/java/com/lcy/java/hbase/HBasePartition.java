package com.lcy.java.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

//创建分区表

public class HBasePartition {
    
    private static Connection connection = null;
    private static HBaseAdmin admin = null;
    
    static {
        //设置配置文件信息
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        
        try {
            //获取连接
            connection = ConnectionFactory.createConnection(conf);
            //获取Admin对象
            admin = (HBaseAdmin) connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    
    public static void main(String[] args) {
    
    }
    
    /**
     * 创建预分区hbase表
     * @param tableName 表名
     * @param columnFamily 列簇
     * @return
     */
    public boolean createTableBySplitKeys(String tableName, List<String> columnFamily) {
        try {
            if (StringUtils.isBlank(tableName) || columnFamily == null || columnFamily.size() < 0) {
                System.out.println("=== 参数 tableName | columnFamily不能为null，请检查！===");
                return false;
            }
            if (admin.tableExists(tableName)) {
                return false;
            } else {
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                for (String cf : columnFamily) {
                    tableDescriptor.addFamily(new HColumnDescriptor(cf));
                }
                byte[][] splitKeys = getSplitKeys();
                admin.createTable(tableDescriptor,splitKeys);//指定splitkeys
                System.out.println("===创建表 " + tableName + " 成功！列族:" + columnFamily.toString() + "===");
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }
    
    private byte[][] getSplitKeys() {
        String[] keys = new String[] { "10|", "20|", "30|", "40|", "50|",
                "60|", "70|", "80|", "90|" };
        byte[][] splitKeys = new byte[keys.length][];
        //先使用TreeSet去除相同的值，并排序
        TreeSet<byte[]> rows = new TreeSet<>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i=0;
        //创建byte[][]
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }
    
    
}
