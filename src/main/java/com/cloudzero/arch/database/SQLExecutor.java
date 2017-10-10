package com.cloudzero.arch.database;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.security.InvalidParameterException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * 数据库连接包装对象 非线程安全
 * Created by lishiwu on 2016/7/15.
 */
public class SQLExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLExecutor.class);
    //每次处理的数据数据量
    private static final int BATCH_OPERATION_SIZE = 5000;
    private Connection connection;

    SQLExecutor(Connection connection) throws SQLException {
        if (connection != null && !connection.isClosed()) {
            this.connection = connection;
        } else {
            throw new InvalidParameterException("Connection cant't be null or closed");
        }
    }

    /**
     * 回滚事务
     */
    public void rollback() {
        try {
            this.connection.rollback();
        } catch (SQLException e) {
            LOGGER.warn(e.getMessage(), e);
        }
    }

    /**
     * 提交事务
     */
    public void commit() {
        try {
            this.connection.commit();
        } catch (SQLException e) {
            LOGGER.warn(e.getMessage(), e);
        }
    }

    /**
     * 关闭数据库连接资源
     */
    public void closeQuietly() {
        this.closeQuietly(this.connection, null, null);
    }

    /**
     * 提交并关闭
     */
    public void commitAndClose() {
        try {
            this.connection.commit();
        } catch (SQLException e) {
            LOGGER.warn(e.getMessage(), e);
        } finally {
            this.closeQuietly(this.connection, null, null);
        }
    }

    /**
     * 回滚并关闭
     */
    public void rollbackAndClose() {
        try {
            this.connection.rollback();
        } catch (SQLException e) {
            LOGGER.warn(e.getMessage(), e);
        } finally {
            this.closeQuietly(this.connection, null, null);
        }
    }

    /**
     * 查询以名值对方式返回记录
     *
     * @param statement
     * @throws Exception
     */
    public List<Map<String, String>> queryMapList(SQLStatement statement) throws SQLException {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = this.connection.prepareStatement(statement.getSql());
            this.fillStatement(preparedStatement, statement.getParamList());
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData rsmd = resultSet.getMetaData();
            Map<String, Integer> rsmdMap = new HashMap<>();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                rsmdMap.put(rsmd.getColumnName(i), rsmd.getColumnType(i));
            }
            List<Map<String, String>> rsList = new ArrayList<>();
            Map<String, String> rowMap;
            int initialCapacity = (int) (rsmd.getColumnCount() / 0.75F + 1.0F);
            while (resultSet.next()) {
                rowMap = new HashMap<>(initialCapacity);
                for (Map.Entry<String, Integer> entry : rsmdMap.entrySet()) {
                    switch (entry.getValue()) {
                        case Types.DATE:
                        case Types.TIMESTAMP:
                            if (resultSet.getTimestamp(entry.getKey()) != null) {
                                rowMap.put(entry.getKey(), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(resultSet.getTimestamp(entry.getKey()).getTime()));
                            } else {
                                rowMap.put(entry.getKey(), null);
                            }
                            break;
                        default:
                            rowMap.put(entry.getKey(), resultSet.getString(entry.getKey()));
                    }
                }
                rsList.add(rowMap);
            }
            return rsList;
        } finally {
            this.closeQuietly(null, preparedStatement, resultSet);
        }
    }

    /**
     * 查询单条以名值对方式返回记录
     *
     * @param statement
     * @throws Exception
     */
    public Map<String, String> queryOneMap(SQLStatement statement) throws SQLException {
        List<Map<String, String>> list = this.queryMapList(statement);
        if (list.size() > 0) {
            return list.get(0);
        } else {
            return null;
        }
    }

    /**
     * 从数据库中读取数据
     *
     * @param statement
     * @param handler
     * @return
     * @throws SQLException
     */
    public <T> T queryOne(SQLStatement statement, RSHandler<T> handler) throws SQLException {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            String sql = statement.getSql();

            if (!sql.toUpperCase().contains("LIMIT ") && sql.toUpperCase().contains("SELECT ")) {
                sql += " LIMIT 1";
            }
            preparedStatement = this.connection.prepareStatement(sql);
            this.fillStatement(preparedStatement, statement.getParamList());
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return handler.parseFrom(resultSet);
            } else {
                return null;
            }
        } finally {
            this.closeQuietly(null, preparedStatement, resultSet);
        }
    }

    /**
     * 从数据库中读取数据
     *
     * @param statement
     * @param handler
     * @return
     * @throws SQLException
     */
    public <T> List<T> queryList(SQLStatement statement, RSHandler<T> handler) throws SQLException {
        List<T> list = new LinkedList<>();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = this.connection.prepareStatement(statement.getSql());
            this.fillStatement(preparedStatement, statement.getParamList());
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                list.add(handler.parseFrom(resultSet));
            }
            return list;
        } finally {
            this.closeQuietly(null, preparedStatement, resultSet);
        }
    }

    /**
     * 数据保存
     *
     * @param t      数据
     * @param parser 参数处理
     * @return
     */
    public <T> int persist(T t, PSParser<T> parser) throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = this.connection.prepareStatement(parser.getSql());
            parser.parseTo(preparedStatement, t);
            return preparedStatement.executeUpdate();
        } finally {
            this.closeQuietly(null, preparedStatement, null);
        }
    }

    /**
     * 数据批量保存
     *
     * @param dataList 数据
     * @param parser   参数处理
     * @return
     */
    public <T> int persist(List<T> dataList, PSParser<T> parser) throws SQLException {
        PreparedStatement preparedStatement = null;
        int hit = 0;
        try {
            preparedStatement = this.connection.prepareStatement(parser.getSql());
            //批量处理
            for (int i = 0, len = dataList.size(); i < len; i++) {
                parser.parseTo(preparedStatement, dataList.get(i));
                preparedStatement.addBatch();
                if (i % BATCH_OPERATION_SIZE == 0) {
                    hit += sumIntArray(preparedStatement.executeBatch());
                    preparedStatement.clearBatch();
                }
            }
            hit += sumIntArray(preparedStatement.executeBatch());
            preparedStatement.clearBatch();
            return hit;
        } finally {
            this.closeQuietly(null, preparedStatement, null);
        }
    }

    /**
     * @param ints
     * @return
     */
    private int sumIntArray(int[] ints) {
        int sum = 0;
        for (int is : ints) {
            sum += is;
        }
        return sum;
    }

    /**
     * 执行数据创建并获取自动生成的key,返回生成的key列表
     *
     * @param statement
     * @return
     * @throws SQLException
     */
    public List<BigDecimal> persistAndGetKey(SQLStatement statement) throws SQLException {
        if (!statement.getSql().toLowerCase().trim().startsWith("insert")) {
            throw new SQLException("Not a insert statement");
        }
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = this.connection.prepareStatement(statement.getSql(), PreparedStatement.RETURN_GENERATED_KEYS);
            this.fillStatement(preparedStatement, statement.getParamList());
            preparedStatement.executeUpdate();
            resultSet = preparedStatement.getGeneratedKeys();
            //由于大多数的insert操作为单条，但是存在多条的可能性
            List<BigDecimal> keyList = new ArrayList<>(1);
            while (resultSet.next()) {
                keyList.add(resultSet.getBigDecimal(1));
            }
            return keyList;
        } finally {
            this.closeQuietly(null, preparedStatement, resultSet);
        }
    }

    /**
     * 执行数据更新
     *
     * @param statement
     * @throws SQLException
     */
    public int persistOrUpdate(SQLStatement statement) throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = this.connection.prepareStatement(statement.getSql());
            this.fillStatement(preparedStatement, statement.getParamList());
            return preparedStatement.executeUpdate();
        } finally {
            this.closeQuietly(null, preparedStatement, null);
        }
    }

    /**
     * 执行数据更新
     *
     * @param statements
     * @throws SQLException
     */
    public int[] persistOrUpdate(List<SQLStatement> statements) throws SQLException {
        int[] hits = new int[statements.size()];
        SQLStatement statement;
        PreparedStatement pstmt = null;
        for (int i = 0; i < statements.size(); i++) {
            statement = statements.get(i);
            try {
                pstmt = this.connection.prepareStatement(statement.getSql());
                this.fillStatement(pstmt, statement.getParamList());
                hits[i] = pstmt.executeUpdate();
            } finally {
                this.closeQuietly(null, pstmt, null);
            }
        }
        return hits;
    }

    /**
     * 关闭数据库资源
     *
     * @param connection
     * @param statement
     * @param resultSet
     */
    private void closeQuietly(Connection connection, Statement statement, ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
        if (connection != null) {
            try {
                if (connection.isReadOnly()) connection.setReadOnly(false);
                if (!connection.getAutoCommit()) connection.setAutoCommit(true);
                connection.close();
            } catch (SQLException e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
    }

    /**
     * 设置PreparedStatement参数
     *
     * @param stmt
     * @param params
     * @throws SQLException
     */
    private void fillStatement(PreparedStatement stmt, List<Object> params)
            throws SQLException {
        ParameterMetaData pmd = stmt.getParameterMetaData();
        int stmtCount = pmd.getParameterCount();
        int paramsCount = params == null ? 0 : params.size();
        if (stmtCount != paramsCount) {
            throw new SQLException("Wrong number of parameters: expected " + stmtCount + ", was given " + paramsCount);
        }
        if (stmtCount == 0 || paramsCount == 0) {
            return;
        }
        for (int i = 0; i < params.size(); i++) {
            int type = pmd.getParameterType(i + 1);
            Object param = params.get(i);
            if (param == null) {
                stmt.setNull(i + 1, type);
            } else {
                switch (type) {
                    case Types.DATE:
                        stmt.setDate(i + 1, new java.sql.Date(((java.util.Date) param).getTime()));
                        break;
                    case Types.TIMESTAMP:
                        stmt.setTimestamp(i + 1, new java.sql.Timestamp(((java.util.Date) param).getTime()));
                        break;
                    default:
                        stmt.setObject(i + 1, params.get(i));
                }
            }
        }
    }

    /**
     * 从数据库中读取数据,并返回总记录数,必须 为 含有limit的非do查询
     *
     * @param statement
     * @param handler
     * @return
     * @throws SQLException
     */
    public <T> PageData<List<T>> queryPageList(SQLStatement statement, RSHandler<T> handler) throws SQLException {
        String sqlCalcFoundRowsQuery = this.generateSqlCalcFoundRows(statement.getSql());
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        PageData<List<T>> pageData = new PageData<>();
        try {
            preparedStatement = this.connection.prepareStatement(sqlCalcFoundRowsQuery);
            this.fillStatement(preparedStatement, statement.getParamList());
            resultSet = preparedStatement.executeQuery();
            List<T> list = new LinkedList<>();
            while (resultSet.next()) {
                list.add(handler.parseFrom(resultSet));
            }
            pageData.data = list;
        } finally {
            this.closeQuietly(null, preparedStatement, resultSet);
        }
        pageData.total = this.queryFoundRows();
        return pageData;
    }

    /**
     * 从数据库中读取数据,并返回总记录数,必须 为 含有limit的非do查询
     *
     * @param statement
     * @return
     * @throws SQLException
     */
    public PageData<List<Map<String, String>>> queryPageMapList(SQLStatement statement) throws SQLException {
        PageData<List<Map<String, String>>> pageData = new PageData<>();
        String sqlCalcFoundRowsQuery = this.generateSqlCalcFoundRows(statement.getSql());
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = this.connection.prepareStatement(sqlCalcFoundRowsQuery);
            this.fillStatement(preparedStatement, statement.getParamList());
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData rsmd = resultSet.getMetaData();
            Map<String, Integer> rsmdMap = new HashMap<>();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                rsmdMap.put(rsmd.getColumnName(i), rsmd.getColumnType(i));
            }
            List<Map<String, String>> rsList = new ArrayList<>();
            Map<String, String> rowMap;
            int initialCapacity = (int) (rsmd.getColumnCount() / 0.75F + 1.0F);
            while (resultSet.next()) {
                rowMap = new HashMap<>(initialCapacity);
                for (Map.Entry<String, Integer> entry : rsmdMap.entrySet()) {
                    switch (entry.getValue()) {
                        case Types.DATE:
                        case Types.TIMESTAMP:
                            if (resultSet.getTimestamp(entry.getKey()) != null) {
                                rowMap.put(entry.getKey(), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(resultSet.getTimestamp(entry.getKey()).getTime()));
                            } else {
                                rowMap.put(entry.getKey(), null);
                            }
                            break;
                        default:
                            rowMap.put(entry.getKey(), resultSet.getString(entry.getKey()));
                    }
                }
                rsList.add(rowMap);
            }
            pageData.data = rsList;
        } finally {
            this.closeQuietly(null, preparedStatement, resultSet);
        }
        pageData.total = this.queryFoundRows();
        return pageData;
    }

    /**
     * 使用FOUND_ROWS函数查询含有limit的查询语句的总记录数
     *
     * @return
     * @throws SQLException
     */

    private int queryFoundRows() throws SQLException {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = this.connection.prepareStatement("SELECT FOUND_ROWS()");
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt(1);
            } else {
                return 0;
            }
        } finally {
            this.closeQuietly(null, preparedStatement, resultSet);
        }
    }

    /**
     * 构造SQL_CALC_FOUND_ROWS查询
     *
     * @param originalSql
     * @return
     */
    private String generateSqlCalcFoundRows(String originalSql) {
        if (!originalSql.toUpperCase().startsWith("SELECT ") || !originalSql.toUpperCase().contains("LIMIT ")) {
            throw new InvalidParameterException("Not a invalid query statement");
        }
        if (originalSql.toUpperCase().contains("SQL_CALC_FOUND_ROWS ")) {
            return originalSql;
        } else {
            return originalSql.substring(0, 7) + "SQL_CALC_FOUND_ROWS " + originalSql.substring(7);
        }
    }
}
