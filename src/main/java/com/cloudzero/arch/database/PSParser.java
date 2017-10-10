package com.cloudzero.arch.database;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by lishiwu on 2016/10/10.
 */
public interface PSParser<T> {
    /**
     * 解析bean到PreparedStatement
     * @param ps
     * @param pojo
     * @throws SQLException
     */
    void parseTo(PreparedStatement ps, T pojo) throws SQLException;

    /**
     * 执行的SQL
     * @return
     */
    String getSql();
}
