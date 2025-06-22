package com.zsy;

import java.sql.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.*;
import java.util.concurrent.locks.*;

/*
* 生成版的数据库连接池类
*
*
* */


public class AdvancedConnectionPool implements ConnectionPool {
    // 配置参数
    private final String url;
    private final String username;
    private final String password;
    private final int maxSize;
    private final int minIdle;
    private final long maxWaitMillis;
    private final long validationInterval;
    private final long leakDetectionThreshold;

    // 连接状态
    private final BlockingQueue<PooledConnection> idleConnections;
    private final Set<PooledConnection> activeConnections = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final AtomicInteger totalConnections = new AtomicInteger(0);

    // 监控统计
    private final AtomicLong connectionRequestCount = new AtomicLong();
    private final AtomicLong connectionTimeoutCount = new AtomicLong();
    private final AtomicLong connectionCreateCount = new AtomicLong();
    private final AtomicLong connectionDestroyCount = new AtomicLong();
    private final AtomicLong connectionValidationFailCount = new AtomicLong();

    // 维护线程池
    private ScheduledExecutorService maintenanceExecutor;
    private volatile boolean isClosed = false;

    // 锁对象
    private final Lock lock = new ReentrantLock();

    public AdvancedConnectionPool(String url, String username, String password,
                                  int minIdle, int maxSize,
                                  long maxWaitMillis, long validationInterval,
                                  long leakDetectionThreshold) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.minIdle = minIdle;
        this.maxSize = maxSize;
        this.maxWaitMillis = maxWaitMillis;
        this.validationInterval = validationInterval;
        this.leakDetectionThreshold = leakDetectionThreshold;
        this.idleConnections = new LinkedBlockingQueue<>(maxSize);

        initialize();
    }

    private void initialize() {
        // 初始化最小空闲连接
        for (int i = 0; i < minIdle; i++) {
            try {
                idleConnections.add(createPooledConnection());
            } catch (SQLException e) {
                throw new ConnectionPoolInitializationException("初始化连接池失败", e);
            }
        }

        // 启动维护线程
        maintenanceExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ConnectionPool-Maintenance");
            t.setDaemon(true);
            return t;
        });

        // 定期执行连接验证和空闲连接补充
        maintenanceExecutor.scheduleWithFixedDelay(() -> {
            try {
                validateIdleConnections();
                fillIdleConnections();
                detectLeakedConnections();
            } catch (Exception e) {
                // 记录日志
            }
        }, validationInterval, validationInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public Connection getConnection() throws SQLException {
        connectionRequestCount.incrementAndGet();

        PooledConnection conn = null;
        long startTime = System.currentTimeMillis();

        try {
            // 1. 尝试从空闲队列获取
            while (conn == null) {
                if (isClosed) {
                    throw new SQLException("连接池已关闭");
                }

                conn = idleConnections.poll();

                if (conn != null) {
                    // 验证连接是否有效
                    if (!validateConnection(conn)) {
                        connectionValidationFailCount.incrementAndGet();
                        quietlyCloseConnection(conn.getRealConnection());
                        conn = null;
                        continue;
                    }

                    // 记录借用时间用于泄漏检测
                    conn.setBorrowTime(System.currentTimeMillis());
                    activeConnections.add(conn);
                    return conn;
                }

                // 2. 如果未达到最大连接数，创建新连接
                if (totalConnections.get() < maxSize) {
                    if (totalConnections.incrementAndGet() <= maxSize) {
                        try {
                            conn = createPooledConnection();
                            conn.setBorrowTime(System.currentTimeMillis());
                            activeConnections.add(conn);
                            connectionCreateCount.incrementAndGet();
                            return conn;
                        } catch (SQLException e) {
                            totalConnections.decrementAndGet();
                            throw e;
                        }
                    } else {
                        totalConnections.decrementAndGet();
                    }
                }

                // 3. 等待可用连接
                if (maxWaitMillis > 0) {
                    try {
                        conn = idleConnections.poll(maxWaitMillis - (System.currentTimeMillis() - startTime),
                                TimeUnit.MILLISECONDS);
                        if (conn == null) {
                            connectionTimeoutCount.incrementAndGet();
                            throw new SQLTimeoutException("获取连接超时，等待时间 " + maxWaitMillis + "ms");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("获取连接被中断", e);
                    }
                } else {
                    throw new SQLException("连接池已耗尽，无法获取连接");
                }
            }
        } finally {
            if (conn == null && totalConnections.get() >= maxSize) {
                connectionTimeoutCount.incrementAndGet();
            }
        }

        return conn;
    }

    @Override
    public void releaseConnection(Connection connection) {
        if (connection == null || isClosed) {
            return;
        }

        if (!(connection instanceof PooledConnection)) {
            quietlyCloseConnection(connection);
            return;
        }

        PooledConnection pooledConn = (PooledConnection) connection;
        if (!activeConnections.remove(pooledConn)) {
            // 连接不属于此连接池
            return;
        }

        // 重置连接状态
        try {
            if (!connection.getAutoCommit()) {
                connection.rollback();
                connection.setAutoCommit(true);
            }

            // 清除警告
            connection.clearWarnings();
        } catch (SQLException e) {
            // 连接可能已损坏，直接关闭
            quietlyCloseConnection(connection);
            return;
        }

        // 如果连接池已关闭或连接无效，直接关闭连接
        if (isClosed || !validateConnection(pooledConn)) {
            quietlyCloseConnection(connection);
            totalConnections.decrementAndGet();
            return;
        }

        // 将连接返回到空闲队列
        if (!idleConnections.offer(pooledConn)) {
            quietlyCloseConnection(connection);
            totalConnections.decrementAndGet();
        }
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }

        isClosed = true;

        // 关闭维护线程
        if (maintenanceExecutor != null) {
            maintenanceExecutor.shutdownNow();
        }

        // 关闭所有连接
        lock.lock();
        try {
            for (PooledConnection conn : idleConnections) {
                quietlyCloseConnection(conn.getRealConnection());
            }
            idleConnections.clear();

            for (PooledConnection conn : activeConnections) {
                quietlyCloseConnection(conn.getRealConnection());
            }
            activeConnections.clear();

            totalConnections.set(0);
        } finally {
            lock.unlock();
        }
    }

    // 创建池化连接
    private PooledConnection createPooledConnection() throws SQLException {
        Connection realConn = DriverManager.getConnection(url, username, password);
        return new PooledConnection(realConn, this);
    }

    // 验证连接有效性
    private boolean validateConnection(PooledConnection pooledConn) {
        try {
            Connection realConn = pooledConn.getRealConnection();
            if (realConn.isClosed()) {
                return false;
            }

            // 简单验证：执行一个快速查询
            try (Statement stmt = realConn.createStatement()) {
                stmt.execute("SELECT 1");
            }
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    // 安静地关闭连接
    private void quietlyCloseConnection(Connection connection) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                connectionDestroyCount.incrementAndGet();
            }
        } catch (SQLException e) {
            // 记录日志
        }
    }

    // 验证空闲连接
    private void validateIdleConnections() {
        List<PooledConnection> invalidConnections = new ArrayList<>();

        for (PooledConnection conn : idleConnections) {
            if (!validateConnection(conn)) {
                invalidConnections.add(conn);
            }
        }

        for (PooledConnection conn : invalidConnections) {
            if (idleConnections.remove(conn)) {
                quietlyCloseConnection(conn.getRealConnection());
                totalConnections.decrementAndGet();
                connectionValidationFailCount.incrementAndGet();
            }
        }
    }

    // 补充空闲连接
    private void fillIdleConnections() {
        while (totalConnections.get() < minIdle && idleConnections.size() < minIdle) {
            try {
                PooledConnection conn = createPooledConnection();
                if (idleConnections.offer(conn)) {
                    totalConnections.incrementAndGet();
                } else {
                    quietlyCloseConnection(conn.getRealConnection());
                }
            } catch (SQLException e) {
                // 记录日志
                break;
            }
        }
    }

    // 检测泄漏连接
    private void detectLeakedConnections() {
        if (leakDetectionThreshold <= 0) {
            return;
        }

        long now = System.currentTimeMillis();
        List<PooledConnection> leakedConnections = new ArrayList<>();

        for (PooledConnection conn : activeConnections) {
            long borrowTime = conn.getBorrowTime();
            if (borrowTime > 0 && (now - borrowTime) > leakDetectionThreshold) {
                leakedConnections.add(conn);
            }
        }

        for (PooledConnection conn : leakedConnections) {
            // 记录泄漏日志
            System.err.println("检测到可能泄漏的连接: " + conn);

            // 强制关闭泄漏连接
            if (activeConnections.remove(conn)) {
                quietlyCloseConnection(conn.getRealConnection());
                totalConnections.decrementAndGet();
            }
        }
    }

    // 池化连接包装类
    private static class PooledConnection implements Connection {
        private final Connection realConnection;
        private final AdvancedConnectionPool pool;
        private volatile long borrowTime;

        public PooledConnection(Connection realConnection, AdvancedConnectionPool pool) {
            this.realConnection = realConnection;
            this.pool = pool;
        }

        public Connection getRealConnection() {
            return realConnection;
        }

        public long getBorrowTime() {
            return borrowTime;
        }

        public void setBorrowTime(long borrowTime) {
            this.borrowTime = borrowTime;
        }

        @Override
        public void close() throws SQLException {
            pool.releaseConnection(this);
        }

        // 以下是Connection接口的所有方法实现
        @Override
        public Statement createStatement() throws SQLException {
            return realConnection.createStatement();
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return realConnection.prepareStatement(sql);
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            return realConnection.prepareCall(sql);
        }

        @Override
        public String nativeSQL(String sql) throws SQLException {
            return realConnection.nativeSQL(sql);
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            realConnection.setAutoCommit(autoCommit);
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return realConnection.getAutoCommit();
        }

        @Override
        public void commit() throws SQLException {
            realConnection.commit();
        }

        @Override
        public void rollback() throws SQLException {
            realConnection.rollback();
        }

        @Override
        public boolean isClosed() throws SQLException {
            return realConnection.isClosed();
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return realConnection.getMetaData();
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            realConnection.setReadOnly(readOnly);
        }

        @Override
        public boolean isReadOnly() throws SQLException {
            return realConnection.isReadOnly();
        }

        @Override
        public void setCatalog(String catalog) throws SQLException {
            realConnection.setCatalog(catalog);
        }

        @Override
        public String getCatalog() throws SQLException {
            return realConnection.getCatalog();
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            realConnection.setTransactionIsolation(level);
        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return realConnection.getTransactionIsolation();
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return realConnection.getWarnings();
        }

        @Override
        public void clearWarnings() throws SQLException {
            realConnection.clearWarnings();
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return realConnection.createStatement(resultSetType, resultSetConcurrency);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return realConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return realConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public Map<String, Class<?>> getTypeMap() throws SQLException {
            return realConnection.getTypeMap();
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            realConnection.setTypeMap(map);
        }

        @Override
        public void setHoldability(int holdability) throws SQLException {
            realConnection.setHoldability(holdability);
        }

        @Override
        public int getHoldability() throws SQLException {
            return realConnection.getHoldability();
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            return realConnection.setSavepoint();
        }

        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            return realConnection.setSavepoint(name);
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            realConnection.rollback(savepoint);
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            realConnection.releaseSavepoint(savepoint);
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return realConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return realConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return realConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return realConnection.prepareStatement(sql, autoGeneratedKeys);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            return realConnection.prepareStatement(sql, columnIndexes);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            return realConnection.prepareStatement(sql, columnNames);
        }

        @Override
        public Clob createClob() throws SQLException {
            return realConnection.createClob();
        }

        @Override
        public Blob createBlob() throws SQLException {
            return realConnection.createBlob();
        }

        @Override
        public NClob createNClob() throws SQLException {
            return realConnection.createNClob();
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            return realConnection.createSQLXML();
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            return realConnection.isValid(timeout);
        }

        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            realConnection.setClientInfo(name, value);
        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            realConnection.setClientInfo(properties);
        }

        @Override
        public String getClientInfo(String name) throws SQLException {
            return realConnection.getClientInfo(name);
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            return realConnection.getClientInfo();
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return realConnection.createArrayOf(typeName, elements);
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return realConnection.createStruct(typeName, attributes);
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return realConnection.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return realConnection.isWrapperFor(iface);
        }

        @Override
        public String toString() {
            return "PooledConnection{" + "realConnection=" + realConnection + '}';
        }
        // JDBC 4.1 新增的方法
        @Override
        public void setSchema(String schema) throws SQLException {
            realConnection.setSchema(schema);
        }

        @Override
        public String getSchema() throws SQLException {
            return realConnection.getSchema();
        }

        // JDBC 4.1 新增的方法
        @Override
        public void abort(Executor executor) throws SQLException {
            realConnection.abort(executor);
        }

        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            realConnection.setNetworkTimeout(executor, milliseconds);
        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            return realConnection.getNetworkTimeout();
        }
    }

    // 连接池统计信息
    public PoolStats getStats() {
        return new PoolStats(
                totalConnections.get(),
                idleConnections.size(),
                activeConnections.size(),
                connectionRequestCount.get(),
                connectionTimeoutCount.get(),
                connectionCreateCount.get(),
                connectionDestroyCount.get(),
                connectionValidationFailCount.get()
        );
    }

    public static class PoolStats {
        private final int totalConnections;
        private final int idleConnections;
        private final int activeConnections;
        private final long connectionRequests;
        private final long connectionTimeouts;
        private final long connectionsCreated;
        private final long connectionsDestroyed;
        private final long validationFailures;

        public PoolStats(int totalConnections, int idleConnections, int activeConnections,
                         long connectionRequests, long connectionTimeouts,
                         long connectionsCreated, long connectionsDestroyed,
                         long validationFailures) {
            this.totalConnections = totalConnections;
            this.idleConnections = idleConnections;
            this.activeConnections = activeConnections;
            this.connectionRequests = connectionRequests;
            this.connectionTimeouts = connectionTimeouts;
            this.connectionsCreated = connectionsCreated;
            this.connectionsDestroyed = connectionsDestroyed;
            this.validationFailures = validationFailures;
        }

        // Getter方法
        public int getTotalConnections() {
            return totalConnections;
        }

        public int getIdleConnections() {
            return idleConnections;
        }

        public int getActiveConnections() {
            return activeConnections;
        }

        public long getConnectionRequests() {
            return connectionRequests;
        }

        public long getConnectionTimeouts() {
            return connectionTimeouts;
        }

        public long getConnectionsCreated() {
            return connectionsCreated;
        }

        public long getConnectionsDestroyed() {
            return connectionsDestroyed;
        }

        public long getValidationFailures() {
            return validationFailures;
        }
    }


    public static class ConnectionPoolInitializationException extends RuntimeException {
        public ConnectionPoolInitializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}