package com.lcy.java.demo.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

public class ConnectionPool {

    private static LinkedList<Connection> connectionQueue;

    static{
        try{
            Class.forName("com.mysql.jdbc.Driver");
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection(){

        try{
            if(connectionQueue == null){
                connectionQueue = new LinkedList<>();
                for (int i = 0; i < 5; i++) {
                    Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test_database?useSSL=false&serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&allowMultiQueries=true",
                            "root", "root");
                    connectionQueue.push(conn);
                }
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    public static void returnConnection(Connection conn){
        connectionQueue.push(conn);
    }
}
