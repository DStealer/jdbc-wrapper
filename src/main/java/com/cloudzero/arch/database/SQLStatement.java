package com.cloudzero.arch.database;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * SQLStatement just for Mysql
 * Created by lishiwu on 2016/7/18.
 */
public class SQLStatement {
    private String sql;
    private List<Object> paramList;

    /**
     * 私有化构造器
     *
     * @param sql
     * @param paramList
     */
    private SQLStatement(String sql, List<Object> paramList) {
        this.sql = sql;
        this.paramList = paramList;
    }

    /**
     * 新建构造器
     *
     * @return
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    public String getSql() {
        return sql;
    }

    public List<Object> getParamList() {
        return paramList;
    }

    @Override
    public String toString() {
        return "SQLStatement{" +
                "sql='" + sql + '\'' +
                ", paramList=" + paramList +
                '}';
    }

    /**
     * SQLStatement构造器
     */
    public static final class Builder {
        private static final int MAX_FETCH_SIZE = 5000;
        private StringBuilder sqlBuilder;
        private List<Object> paramList;

        private Builder() {
            this.sqlBuilder = new StringBuilder(32);
            this.paramList = new ArrayList<Object>();
        }

        /**
         * 添加statement参数
         *
         * @param statement
         * @return
         */
        public Builder append(SQLStatement statement) {
            this.sqlBuilder.append(statement.getSql());
            this.paramList.add(statement.getParamList());
            return this;
        }

        /**
         * 添加statement参数
         *
         * @param statement
         * @return
         */
        public Builder prepend(SQLStatement statement) {
            this.sqlBuilder.insert(0, statement.getSql());
            this.paramList.addAll(0, statement.getParamList());
            return this;
        }

        /**
         * 添加sqlSegment
         *
         * @param sqlSegment
         * @return
         */
        public Builder append(String sqlSegment) {
            this.sqlBuilder.append(sqlSegment);
            return this;
        }

        /**
         * 条件添加sqlSegment
         *
         * @param sqlSegment
         * @return
         */
        public Builder append(boolean condition, String sqlSegment) {
            if (condition) {
                this.sqlBuilder.append(sqlSegment);
            }
            return this;
        }

        /**
         * 添加sqlSegment
         *
         * @param sqlSegment
         * @return
         */
        public Builder prepend(String sqlSegment) {
            this.sqlBuilder.insert(0, sqlSegment);
            return this;
        }

        /**
         * 条件添加sqlSegment
         *
         * @param sqlSegment
         * @return
         */
        public Builder prepend(boolean condition, String sqlSegment) {
            if (condition) {
                this.sqlBuilder.insert(0, sqlSegment);
            }
            return this;
        }

        /**
         * sql是否为空
         *
         * @return
         */
        public boolean isEmpty() {
            return this.sqlBuilder.length() == 0;
        }

        /**
         * sql是否不为空
         *
         * @return
         */
        public boolean isNotEmpty() {
            return this.sqlBuilder.length() > 0;
        }

        /**
         * 清空参数
         */
        public void clear() {
            this.sqlBuilder = new StringBuilder(32);
            this.paramList = new ArrayList<Object>();
        }

        /**
         * 添加sqlSegment
         *
         * @param sqlSegment
         * @return
         */
        public Builder append(String sqlSegment, Object... params) {
            this.sqlBuilder.append(sqlSegment);
            if (params != null && params.length > 0) {
                Collections.addAll(this.paramList, params);
            }
            return this;
        }

        /**
         * 条件添加sqlSegment
         *
         * @param sqlSegment
         * @return
         */
        public Builder append(boolean condition, String sqlSegment, Object... params) {
            if (condition && params != null && params.length > 0) {
                this.append(sqlSegment, params);
            }
            return this;
        }

        /**
         * 添加in参数和sqlSegment
         *
         * @param sqlSegment
         * @param params
         * @return
         */
        public Builder appendIn(String sqlSegment, Object... params) {
            this.append(true, sqlSegment, params);
            return this;
        }

        /**
         * 条件添加in参数和sqlSegment
         *
         * @param sqlSegment
         * @return
         */
        public Builder appendIn(boolean condition, String sqlSegment, Object... params) {
            if (condition && params != null && params.length > 0) {
                StringBuilder builder = new StringBuilder("?");
                for (int i = 0; i < params.length - 1; i++) {
                    builder.append(",?");
                }
                this.append(sqlSegment.replace("?", builder.toString()), params);
            }
            return this;
        }

        /**
         * 添加limit限制
         *
         * @param indexFrom
         * @param limit
         * @return
         */
        public Builder limit(int indexFrom, int limit) {
            if (indexFrom < 0) {
                indexFrom = 0;
            }
            if (limit < 0 || limit > MAX_FETCH_SIZE) {
                limit = MAX_FETCH_SIZE;
            }
            this.sqlBuilder.append(" LIMIT ").append(indexFrom).append(",").append(limit);
            return this;
        }

        /**
         * 添加limit限制 1
         *
         * @return
         */
        public Builder limitOne() {
            this.sqlBuilder.append(" LIMIT 1");
            return this;
        }

        /**
         * 构建
         *
         * @return
         */
        public SQLStatement build() {
            return new SQLStatement(this.sqlBuilder.toString(), Collections.unmodifiableList(this.paramList));
        }
    }
}
