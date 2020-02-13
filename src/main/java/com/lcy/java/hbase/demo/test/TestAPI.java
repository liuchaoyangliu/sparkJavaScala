package com.lcy.java.hbase.demo.test;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class TestAPI {
    private static Connection connection = null;
    private static Admin admin = null;
    
    public TestAPI() {
    }
    
    public static boolean isTableExist(String tableName) throws IOException {
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }
    
    public static void createTable(String tableName, String... cfs) throws IOException {
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息！");
        } else if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在！");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            String[] var3 = cfs;
            int var4 = cfs.length;
            
            for(int var5 = 0; var5 < var4; ++var5) {
                String cf = var3[var5];
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            
            admin.createTable(hTableDescriptor);
        }
    }
    
    public static void dropTable(String tableName) throws IOException {
        if (!isTableExist(tableName)) {
            System.out.println(tableName + "表不存在！！！");
        } else {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }
    }
    
    public static void createNameSpace(String ns) {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException var3) {
            System.out.println(ns + "命名空间已存在！");
        } catch (IOException var4) {
            var4.printStackTrace();
        }
        
        System.out.println("哈哈哈，尽管存在，我还是可以走到这！！");
    }
    
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sex"), Bytes.toBytes("male"));
        table.put(put);
        table.close();
    }
    
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        get.setMaxVersions(5);
        Result result = table.get(get);
        Cell[] var7 = result.rawCells();
        int var8 = var7.length;
        
        for(int var9 = 0; var9 < var8; ++var9) {
            Cell cell = var7[var9];
            System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "，CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "，Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        
        table.close();
    }
    
    public static void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1002"));
        ResultScanner resultScanner = table.getScanner(scan);
        Iterator var4 = resultScanner.iterator();
        
        while(var4.hasNext()) {
            Result result = (Result)var4.next();
            Cell[] var6 = result.rawCells();
            int var7 = var6.length;
            
            for(int var8 = 0; var8 < var7; ++var8) {
                Cell cell = var6[var8];
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) + "，CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "，CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "，Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        
        table.close();
    }
    
    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addFamily(Bytes.toBytes(cf));
        table.delete(delete);
        table.close();
    }
    
    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException var2) {
                var2.printStackTrace();
            }
        }
        
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException var1) {
                var1.printStackTrace();
            }
        }
        
    }
    
    public static void main(String[] args) throws IOException {
        deleteData("stu", "1009", "info1", "name");
        close();
    }
    
    static {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException var1) {
            var1.printStackTrace();
        }
        
    }
}

