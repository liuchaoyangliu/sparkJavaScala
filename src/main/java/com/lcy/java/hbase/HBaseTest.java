package com.lcy.java.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseTest {
    
    private static Connection connection;
    private static Table table;
    private static HBaseAdmin admin;
    
    static {
        //获取配置信息
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.166.9.102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            //获取连接
            connection = ConnectionFactory.createConnection(conf);
            //获取admin对象
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

        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
            //System.exit(0);
        } else {
            //创建表属性对象,表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功！");
        }
    }
    
    //删除表
    public static void dropTable(String tableName) throws IOException {
        if (isTableExist(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在！");
        }
    }
    
    //向表中插入数据
    public static void addRowData(String tableName,
                                  String rowKey,
                                  String columnFamily,
                                  String column,
                                  String value) throws IOException {
        //创建HTable对象
        table = connection.getTable(TableName.valueOf(tableName));
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向Put对象中组装数据
        put.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes());
        table.put(put);
        table.close();
        System.out.println("插入数据成功");
    }
    
    //删除多行数据
    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        //创建HTable对象
        table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
    }
    
    //获取所有数据
    public static void getAllRows(String tableName) throws IOException {
        table = connection.getTable(TableName.valueOf(tableName));
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable得到resultScanner实现类的对象
        ResultScanner resultScanner = table.getScanner(scan);
        
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("	行	键	:" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("	列	族	" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("	列	:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("	值	:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
    
    //获取某一行数据
    public static void getRow(String tableName, String rowKey) throws IOException {
        table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("	行	键	:" + result.getRow().toString());
            System.out.println("	列	族	" + CellUtil.cloneFamily(cell).toString());
            System.out.println("	列	:" + CellUtil.cloneQualifier(cell).toString());
            System.out.println("	值	:" + CellUtil.cloneValue(cell).toString());
            System.out.println("    时间戳:" + cell.getTimestamp());
        }
    }
    
    //获取某一行指定“列族:列”的数据
    public static void getRowQualifier(String tableName,
                                       String rowKey,
                                       String family,
                                       String qualifier) throws IOException {
        table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(family.getBytes(), qualifier.getBytes());
        
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("	行	键	:" + result.getRow().toString());
            System.out.println("	列	族	" + CellUtil.cloneFamily(cell).toString());
            System.out.println("	列	:" + CellUtil.cloneQualifier(cell).toString());
            System.out.println("	值	:" + CellUtil.cloneValue(cell).toString());
        }
    }
    
    public static void close() {
        if(admin != null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(connection != null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
}
