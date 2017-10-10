package com.cloudzero.arch.database;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

/**
 * 简单类型处理
 * Created by LiShiwu on 03/08/2017.
 */
public class SimpleResult {
    /**
     * 数字类型
     */
    public static final RSHandler<Number> NumberResult = new RSHandler<Number>() {
        @Override
        public Number parseFrom(ResultSet resultSet) throws SQLException {
            return resultSet.getBigDecimal(1);
        }
    };
    /**
     * 字符串类型
     */
    public static final RSHandler<String> StringResult = new RSHandler<String>() {
        @Override
        public String parseFrom(ResultSet resultSet) throws SQLException {
            return resultSet.getString(1);
        }
    };
    /**
     * 日期类型
     */
    public static final RSHandler<Date> DateResult = new RSHandler<Date>() {
        @Override
        public Date parseFrom(ResultSet resultSet) throws SQLException {
            return resultSet.getTimestamp(1);
        }
    };
}
