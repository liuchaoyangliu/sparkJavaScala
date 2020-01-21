package com.lcy.java.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.List;

public class HBaseTest2 {
    
    private static Connection connection;
    private static HBaseAdmin admin;
    
    
    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.153.128");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = (HBaseAdmin) connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    //查询所有表
    public static void getAllTables() {
        try {
            HTableDescriptor[] listTables = admin.listTables();
            for (HTableDescriptor table : listTables) {
                String nameAsString = table.getNameAsString();
                System.out.println("表名: " + nameAsString);
                HColumnDescriptor[] columnFamilies = table.getColumnFamilies();
                for (HColumnDescriptor columnFamily : columnFamilies) {
                    String columnFamilyName = columnFamily.getNameAsString();
                    System.out.print(" 列族名：" + columnFamilyName);
                }
                System.out.println();
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    //创建表
    public static void createTable(String tableName, List<String> family) {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String str : family) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
            tableDescriptor.addFamily(hColumnDescriptor);
        }
        try {
            admin.createTable(tableDescriptor);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void createTable(String tableName, String... family) {
        List<String> strings = Arrays.asList(family);
        createTable(tableName, family);
    }
    
    //查看表的列族属性
    public static void descTable(String tableName) {
        HTableDescriptor tableDescriptor = null;
        try {
            tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
        } catch (Exception e) {
            e.printStackTrace();
        }
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        for (HColumnDescriptor column : columnFamilies) {
            System.out.println(column);
        }
    }
    
    //删除表
    public static void deleteTable(String tableName){
        TableName name = TableName.valueOf(tableName);
        try{
            admin.disableTable(name);
            admin.deleteTable(name);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    
    
    
    public static void insertData(String tableName,
                                  String rowKey,
                                  String columnFamily,
                                  String column,
                                  String value){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
            table.put(put);
            table.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    
    
    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


//    public static void close() {
//        if (admin != null) {
//            try {
//                admin.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        if (connection != null) {
//            try {
//                connection.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }

}
