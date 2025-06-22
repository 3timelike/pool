package com.zsy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/*
* 简易版的连接池类
*
* */
public class SimpleConnectionPool {
    // 连接池配置
    private final String url;           //url地址
    private final String username;      //用户名
    private final String password;      //密码
    private final int maxSize;          // 最大连接数
    private final int initialSize;      // 初始连接数
    private final long maxWaitMillis;    // 获取连接最大等待时间(毫秒)

    // 连接池状态
    private final BlockingQueue<Connection> idleConnections; // 空闲连接
    private final AtomicInteger activeCount = new AtomicInteger(0); // 活跃连接数

    //初始化一个连接池
    public SimpleConnectionPool(String url, String username, String password,
                                int initialSize, int maxSize, long maxWaitMillis)
            throws SQLException {
        this.url = url;
        this.username = username;
        this.password = password;
        this.initialSize = initialSize;
        this.maxSize = maxSize;
        this.maxWaitMillis = maxWaitMillis;
        this.idleConnections = new LinkedBlockingQueue<>(maxSize);

        // 初始化连接池
        init();
    }

    // 初始化连接池
    private void init() throws SQLException {
        for (int i = 0; i < initialSize; i++) {
            idleConnections.add(createConnection());
        }
    }

    // 创建新连接
    private Connection createConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    // 获取连接
    public Connection getConnection() throws SQLException {
        Connection conn = null;

        // 1. 首先尝试从空闲队列获取
        conn = idleConnections.poll();
        if (conn != null) {
            activeCount.incrementAndGet();
            return conn;
        }

        // 2. 如果当前活跃连接数小于最大值，创建新连接
        if (activeCount.get() < maxSize) {
            if (activeCount.incrementAndGet() <= maxSize) {
                try {
                    return createConnection();
                } catch (SQLException e) {
                    activeCount.decrementAndGet();
                    throw e;
                }
            } else {
                activeCount.decrementAndGet();
            }
        }

        // 3. 等待可用连接
        try {
            conn = idleConnections.poll(maxWaitMillis, TimeUnit.MILLISECONDS);
            if (conn != null) {
                activeCount.incrementAndGet();
                return conn;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("等待线程被种植了", e.getMessage());
        }

        throw new SQLException("等待时间超时，请重试 " + maxWaitMillis + "ms");
    }

    // 释放连接(返回到连接池)
    public void releaseConnection(Connection conn) {
        if (conn != null) {
            activeCount.decrementAndGet();
            if (!idleConnections.offer(conn)) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // 关闭失败处理
                }
            }
        }
    }

    // 关闭连接池
    public void close() {
        Connection conn;
        while ((conn = idleConnections.poll()) != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // 关闭失败处理
                throw  new RuntimeException("当前连接关闭失败");
            }
        }
    }

    // 获取连接池状态
    public void printStats() {
        System.out.println("Connection pool status:");
        System.out.println(" - Active connections: " + activeCount.get());
        System.out.println(" - Idle connections: " + idleConnections.size());
        System.out.println(" - Max connections: " + maxSize);
    }
}
