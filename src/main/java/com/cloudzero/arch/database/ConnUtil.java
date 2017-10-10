package com.cloudzero.arch.database;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by lishiwu on 2016/7/15.
 */
public class ConnUtil {
    /**
     * 获取非自动commit的数据库连接
     *
     * @param driverClass
     * @param url
     * @param user
     * @param password
     * @return
     * @throws Exception
     */
    public static SQLExecutor getTXExecutor(String driverClass, String url, String user, String password) throws Exception {
        Class.forName(driverClass);
        Connection conn = DriverManager.getConnection(url, user, password);
        conn.setAutoCommit(false);
        return new SQLExecutor(conn);
    }

    /**
     * 获取自动commit的数据库连接
     *
     * @throws Exception
     */
    public static SQLExecutor getExecutor(String driverClass, String url, String user, String password) throws Exception {
        Class.forName(driverClass);
        Connection conn = DriverManager.getConnection(url, user, password);
        conn.setReadOnly(true);
        return new SQLExecutor(conn);
    }
}
