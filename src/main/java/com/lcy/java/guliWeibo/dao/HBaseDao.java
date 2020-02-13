package com.lcy.java.guliWeibo.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import com.lcy.java.guliWeibo.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDao {
    public HBaseDao() {
    }
    
    public static void publishWeiBo(String uid, String content) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table contTable = connection.getTable(TableName.valueOf("weibo:content"));
        long ts = System.currentTimeMillis();
        String rowKey = uid + "_" + ts;
        Put contPut = new Put(Bytes.toBytes(rowKey));
        contPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));
        contTable.put(contPut);
        Table relaTable = connection.getTable(TableName.valueOf("weibo:relation"));
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));
        Result result = relaTable.get(get);
        ArrayList<Put> inboxPuts = new ArrayList();
        Cell[] var12 = result.rawCells();
        int var13 = var12.length;
        
        for(int var14 = 0; var14 < var13; ++var14) {
            Cell cell = var12[var14];
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            inboxPuts.add(inboxPut);
        }
        
        if (inboxPuts.size() > 0) {
            Table inboxTable = connection.getTable(TableName.valueOf("weibo:inbox"));
            inboxTable.put(inboxPuts);
            inboxTable.close();
        }
        
        relaTable.close();
        contTable.close();
        connection.close();
    }
    
    public static void addAttends(String uid, String... attends) throws IOException {
        if (attends.length <= 0) {
            System.out.println("请选择待关注的人！！！");
        } else {
            Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
            Table relaTable = connection.getTable(TableName.valueOf("weibo:relation"));
            ArrayList<Put> relaPuts = new ArrayList();
            Put uidPut = new Put(Bytes.toBytes(uid));
            String[] var6 = attends;
            int var7 = attends.length;
            
            for(int var8 = 0; var8 < var7; ++var8) {
                String attend = var6[var8];
                uidPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend), Bytes.toBytes(attend));
                Put attendPut = new Put(Bytes.toBytes(attend));
                attendPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
                relaPuts.add(attendPut);
            }
            
            relaPuts.add(uidPut);
            relaTable.put(relaPuts);
            Table contTable = connection.getTable(TableName.valueOf("weibo:content"));
            Put inboxPut = new Put(Bytes.toBytes(uid));
            String[] var20 = attends;
            int var22 = attends.length;
            
            for(int var23 = 0; var23 < var22; ++var23) {
                String attend = var20[var23];
                Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
                ResultScanner resultScanner = contTable.getScanner(scan);
                long ts = System.currentTimeMillis();
                Iterator var16 = resultScanner.iterator();
                
                while(var16.hasNext()) {
                    Result result = (Result)var16.next();
                    inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attend), ts++, result.getRow());
                }
            }
            
            if (!inboxPut.isEmpty()) {
                Table inboxTable = connection.getTable(TableName.valueOf("weibo:inbox"));
                inboxTable.put(inboxPut);
                inboxTable.close();
            }
            
            relaTable.close();
            contTable.close();
            connection.close();
        }
    }
    
    public static void deleteAttends(String uid, String... dels) throws IOException {
        if (dels.length <= 0) {
            System.out.println("请添加待取关的用户！！！");
        } else {
            Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
            Table relaTable = connection.getTable(TableName.valueOf("weibo:relation"));
            ArrayList<Delete> relaDeletes = new ArrayList();
            Delete uidDelete = new Delete(Bytes.toBytes(uid));
            String[] var6 = dels;
            int var7 = dels.length;
            
            for(int var8 = 0; var8 < var7; ++var8) {
                String del = var6[var8];
                uidDelete.addColumns(Bytes.toBytes("attends"), Bytes.toBytes(del));
                Delete delDelete = new Delete(Bytes.toBytes(del));
                delDelete.addColumns(Bytes.toBytes("fans"), Bytes.toBytes(uid));
                relaDeletes.add(delDelete);
            }
            
            relaDeletes.add(uidDelete);
            relaTable.delete(relaDeletes);
            Table inboxTable = connection.getTable(TableName.valueOf("weibo:inbox"));
            Delete inboxDelete = new Delete(Bytes.toBytes(uid));
            String[] var15 = dels;
            int var16 = dels.length;
            
            for(int var12 = 0; var12 < var16; ++var12) {
                String del = var15[var12];
                inboxDelete.addColumns(Bytes.toBytes("info"), Bytes.toBytes(del));
            }
            
            inboxTable.delete(inboxDelete);
            relaTable.close();
            inboxTable.close();
            connection.close();
        }
    }
    
    public static void getInit(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table inboxTable = connection.getTable(TableName.valueOf("weibo:inbox"));
        Table contTable = connection.getTable(TableName.valueOf("weibo:content"));
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);
        Cell[] var6 = result.rawCells();
        int var7 = var6.length;
        
        for(int var8 = 0; var8 < var7; ++var8) {
            Cell cell = var6[var8];
            Get contGet = new Get(CellUtil.cloneValue(cell));
            Result contResult = contTable.get(contGet);
            Cell[] var12 = contResult.rawCells();
            int var13 = var12.length;
            
            for(int var14 = 0; var14 < var13; ++var14) {
                Cell contCell = var12[var14];
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(contCell)) + "，CF:" + Bytes.toString(CellUtil.cloneFamily(contCell)) + "，CN:" + Bytes.toString(CellUtil.cloneQualifier(contCell)) + "，Value:" + Bytes.toString(CellUtil.cloneValue(contCell)));
            }
        }
        
        inboxTable.close();
        contTable.close();
        connection.close();
    }
    
    public static void getWeiBo(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table table = connection.getTable(TableName.valueOf("weibo:content"));
        Scan scan = new Scan();
        RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(uid + "_"));
        scan.setFilter(rowFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        Iterator var6 = resultScanner.iterator();
        
        while(var6.hasNext()) {
            Result result = (Result)var6.next();
            Cell[] var8 = result.rawCells();
            int var9 = var8.length;
            
            for(int var10 = 0; var10 < var9; ++var10) {
                Cell cell = var8[var10];
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) + "，CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "，CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "，Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        
        table.close();
        connection.close();
    }
}

