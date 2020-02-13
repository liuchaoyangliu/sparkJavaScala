package com.lcy.java.guliWeibo.test;

import com.lcy.java.guliWeibo.dao.HBaseDao;
import com.lcy.java.guliWeibo.utils.HBaseUtil;

import java.io.IOException;

public class TestWeiBo {
    public TestWeiBo() {
    }
    
    public static void init() {
        try {
            HBaseUtil.createNameSpace("weibo");
            HBaseUtil.createTable("weibo:content", 1, new String[]{"info"});
            HBaseUtil.createTable("weibo:relation", 1, new String[]{"attends", "fans"});
            HBaseUtil.createTable("weibo:inbox", 2, new String[]{"info"});
        } catch (IOException var1) {
            var1.printStackTrace();
        }
        
    }
    
    public static void main(String[] args) throws IOException, InterruptedException {
        init();
        HBaseDao.publishWeiBo("1001", "赶紧下课吧！！！");
        HBaseDao.addAttends("1002", new String[]{"1001", "1003"});
        HBaseDao.getInit("1002");
        System.out.println("************111************");
        HBaseDao.publishWeiBo("1003", "谁说的赶紧下课！！！");
        Thread.sleep(10L);
        HBaseDao.publishWeiBo("1001", "我没说话！！！");
        Thread.sleep(10L);
        HBaseDao.publishWeiBo("1003", "那谁说的！！！");
        Thread.sleep(10L);
        HBaseDao.publishWeiBo("1001", "反正飞机是下线了！！！");
        Thread.sleep(10L);
        HBaseDao.publishWeiBo("1003", "你们爱咋咋地！！！");
        HBaseDao.getInit("1002");
        System.out.println("************222************");
        HBaseDao.deleteAttends("1002", new String[]{"1003"});
        HBaseDao.getInit("1002");
        System.out.println("************333************");
        HBaseDao.addAttends("1002", new String[]{"1003"});
        HBaseDao.getInit("1002");
        System.out.println("************444************");
        HBaseDao.getWeiBo("1001");
    }
}

