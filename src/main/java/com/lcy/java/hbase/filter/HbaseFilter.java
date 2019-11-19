package com.lcy.java.hbase.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseFilter {
    
    private static Connection connection = null;
    private static HBaseAdmin admin = null;
    
    public static void main(String[] args) {
    
    
    }
    
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
    
    
    //行键过滤器 RowFilter
    public static void filter1() throws IOException {
        
        Table table = connection.getTable(TableName.valueOf("student"));
        Scan scan = new Scan();
        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER,
                new BinaryComparator("95007".getBytes()));
        scan.setFilter(rowFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(cell);
            }
        }
        
    }
    
    //列簇过滤器 FamilyFilter
    public static void filter2() throws IOException {
        
        Table table = connection.getTable(TableName.valueOf("student"));
        Scan scan = new Scan();
        Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator("info".getBytes()));
        scan.setFilter(familyFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(cell);
            }
        }
        
    }
    
    //列过滤器 QualifierFilter
    public static void filter3() throws IOException {
        
        Table table = connection.getTable(TableName.valueOf("student"));
        Scan scan = new Scan();
        Filter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator("name".getBytes()));
        scan.setFilter(qualifierFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(cell);
            }
        }
    }
    
    //值过滤器 ValueFilter
    public static void filter4() throws IOException {
        
        Table table = connection.getTable(TableName.valueOf("student"));
        Scan scan = new Scan();
        Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("男"));
        scan.setFilter(valueFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(cell);
            }
        }
    }
    
    //时间戳过滤器 TimestampsFilter
    public static void filter5() throws IOException {
        
        Table table = connection.getTable(TableName.valueOf("student"));
        Scan scan = new Scan();
        List<Long> list = new ArrayList<>();
        list.add(1522469029503l);
        TimestampsFilter timestampsFilter = new TimestampsFilter(list);
        scan.setFilter(timestampsFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell))
                        + "\t" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "\t" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "\t" + Bytes.toString(CellUtil.cloneValue(cell))
                        + "\t" + cell.getTimestamp());
            }
        }
        
    }
    
    
    //专用过滤器
    
    //单列值过滤器 SingleColumnValueFilter ----会返回满足条件的整行
    public static void filter6() throws IOException {
        
        Table table = connection.getTable(TableName.valueOf("student"));
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                "info".getBytes(),
                "name".getBytes(),
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("刘晨"));
        
        singleColumnValueFilter.setFilterIfMissing(true);
        
        scan.setFilter(singleColumnValueFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell))
                                + "\t" + Bytes.toString(CellUtil.cloneFamily(cell))
                                + "\t" + Bytes.toString(CellUtil.cloneQualifier(cell))
                                + "\t" + Bytes.toString(CellUtil.cloneValue(cell))
                                + "\t" + cell.getTimestamp());
            }
        }
        
    }
    
    //单列值排除器 SingleColumnValueExcludeFilter
    public static void filter7() throws IOException {
        
        Table table = connection.getTable(TableName.valueOf("student"));
        Scan scan = new Scan();
        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter =
                new SingleColumnValueExcludeFilter("info".getBytes(),
                        "name".getBytes(),
                        CompareFilter.CompareOp.EQUAL,
                        new SubstringComparator("刘晨"));
        
        singleColumnValueExcludeFilter.setFilterIfMissing(true);
        
        scan.setFilter(singleColumnValueExcludeFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getRow())
                        + "\t" + Bytes.toString(cell.getFamily())
                        + "\t" + Bytes.toString(cell.getQualifier())
                        + "\t" + Bytes.toString(cell.getValue())
                        + "\t" + cell.getTimestamp());
            }
        }
        
    }
    
    //前缀过滤器 PrefixFilter----针对行键
    public static void filter8() throws IOException {
        
        Table table = connection.getTable(TableName.valueOf("student"));
        Scan scan = new Scan();
        PrefixFilter prefixFilter = new PrefixFilter("9501".getBytes());
        scan.setFilter(prefixFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                        + "\t" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "\t" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "\t" + Bytes.toString(CellUtil.cloneValue(cell))
                        + "\t" + cell.getTimestamp());
            }
        }
        
    }
    
    
    //列前缀过滤器 ColumnPrefixFilter
    public static void filter9() throws IOException {
        
        Table table = connection.getTable(TableName.valueOf("student"));
        Scan scan = new Scan();
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter("name".getBytes());
        scan.setFilter(columnPrefixFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                        + "\t" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "\t" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "\t" + Bytes.toString(CellUtil.cloneValue(cell))
                        + "\t" + cell.getTimestamp());
            }
        }
        
    }
    
    
}
