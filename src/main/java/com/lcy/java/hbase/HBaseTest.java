package com.lcy.java.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;

public class HBaseTest {
    
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
    
    //判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {
        return admin.tableExists(tableName);
    }
    
    //创建表
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        if (isTableExist(tableName)) {
            System.out.println("表：" + tableName + " 已经存在");
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            admin.createTable(descriptor);
            System.out.println("表：" + tableName + " 已经存在");
        }
    }
    
    //删除表
    public static void deleteTable(String tableName) throws IOException {
        if (isTableExist(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } else {
            System.out.println("表： " + tableName + "表不存在");
        }
    }
    
    //向表中插入一条数据
    public static void addRowData(String tableName,
                                  String rowKey,
                                  String columnFamily,
                                  String column,
                                  String value) throws IOException {
        if (isTableExist(tableName)) {
            //创建table对象
            Table table = connection.getTable(TableName.valueOf(tableName));
            //封装数据
            Put put = new Put(rowKey.getBytes());
            put.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes());
            //向表中插入数据
            table.put(put);
            //关闭连接
            table.close();
            System.out.println("插入数据成功！");
            
        } else {
            System.out.println("表：" + tableName + " 不存在");
        }
    }
    
    //删除数据 一行或多行
    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        if (isTableExist(tableName)) {
            //获取Table对象
            Table table = connection.getTable(TableName.valueOf(tableName));
            ArrayList<Delete> deleteList = new ArrayList<>();
            for (String row :
                    rows) {
                Delete delete = new Delete(row.getBytes());
                deleteList.add(delete);
            }
            //删除数据
            table.delete(deleteList);
            table.close();
            System.out.println("删除" + tableName + "中的数据成功");
        } else {
            System.out.println("表：" + tableName + "不存在！");
        }
    }
    
    //获取某一行数据
    public static void getRow(String tableName, String rowKey) throws IOException {
        if (isTableExist(tableName)) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println("行键：" + result.getRow().toString());
                System.out.println("列族：" + CellUtil.cloneFamily(cell).toString());
                System.out.println("列：" + CellUtil.cloneQualifier(cell).toString());
                System.out.println("值：" + CellUtil.cloneValue(cell).toString());
                System.out.println("时间戳：" + cell.getTimestamp());
            }
        } else {
            System.out.println("表：" + tableName + " 不存在！");
        }
    }
    
    //获取一张表所有数据
    public static void getAllrRow(String tableName) throws IOException {
        if (isTableExist(tableName)) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.println("行键：" + CellUtil.cloneRow(cell).toString());
                    System.out.println("列族：" + CellUtil.cloneFamily(cell).toString());
                    System.out.println("列：" + CellUtil.cloneQualifier(cell).toString());
                    System.out.println("值：" + CellUtil.cloneValue(cell).toString());
                }
            }
        } else {
            System.out.println("表：" + tableName + "不存在！");
        }
    }
    
    //获取指定“列族：列”的数据
    public static void getRowQualifier(String tableName,
                                       String rowKey,
                                       String family,
                                       String qualifier) throws IOException {
        if (isTableExist(tableName)) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            
            get.addColumn(family.getBytes(), qualifier.getBytes());
            
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println("行键：" + CellUtil.cloneRow(cell).toString());
                System.out.println("列族：" + CellUtil.cloneFamily(cell).toString());
                System.out.println("列：" + CellUtil.cloneQualifier(cell).toString());
                System.out.println("值：" + CellUtil.cloneValue(cell).toString());
            }
        } else {
            System.out.println("表：" + tableName + "不存在");
        }
    }
    
    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
}
