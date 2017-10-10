package com.cloudzero.arch.database;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by lishiwu on 2016/7/14.
 */
public interface RSHandler<T> {
    /**
     * 解析结果集到bean
     *
     * @param resultSet
     * @return
     * @throws SQLException
     */
    T parseFrom(ResultSet resultSet) throws SQLException;
}
