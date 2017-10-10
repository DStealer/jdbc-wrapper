package com.cloudzero.arch.database;

/**
 * 非线程安全
 * <p>
 * Created by lishiwu on 2016/7/22.
 */
public class SQLBuilder {
    private StringBuilder stringBuilder;

    public SQLBuilder() {
        this(32);
    }

    public SQLBuilder(int capacity) {
        this.stringBuilder = new StringBuilder(capacity);
    }

    /**
     * 拼接sql语句
     *
     * @param sqlSegment
     * @return
     */
    public SQLBuilder append(String sqlSegment) {
        this.stringBuilder.append(sqlSegment);
        return this;
    }

    /**
     * 拼接sql语句
     *
     * @param sqlSegment
     * @return
     */
    public SQLBuilder prepend(String sqlSegment) {
        this.stringBuilder.insert(0, sqlSegment);
        return this;
    }

    /**
     * 拼接sql语句
     *
     * @param parameter
     * @param fieldType
     * @return
     */
    public SQLBuilder append(Object parameter, FiledType fieldType) {
        append("", parameter, fieldType);
        return this;
    }

    /**
     * 根据数据库字段类型, 拼接sql语句
     *
     * @param sqlSegment
     * @param parameter
     * @param fieldType
     * @return
     */
    public SQLBuilder append(String sqlSegment, Object parameter, FiledType fieldType) {
        this.stringBuilder.append(sqlSegment);
        switch (fieldType) {
            case INT:
            case DOUBLE:
            case FLOAT:
            case FUNCTION:
            case BOOLEAN:
                this.stringBuilder.append(parameter);
                break;
            case STRING:
            case DATETIME:
                if (parameter == null) {
                    this.stringBuilder.append("null");
                } else {
                    this.stringBuilder.append("'").append(parameter.toString().replaceAll("\'", "").replaceAll("\'\'", "")).append("'");
                }
                break;
        }
        return this;
    }

    /**
     * 根据数据库字段类型, 拼接sql语句
     *
     * @param condition
     * @param sqlSegment
     * @param parameter
     * @param field_type
     * @return
     */
    public SQLBuilder append(boolean condition, String sqlSegment, Object parameter, FiledType field_type) {
        if (condition) {
            append(sqlSegment, parameter, field_type);
        }
        return this;
    }

    /**
     * 构建语句
     *
     * @return
     */
    @Override
    public String toString() {
        return this.stringBuilder.toString();
    }

    /**
     * 判断SQL是否为空
     *
     * @return
     */
    public boolean isEmpty() {
        return this.stringBuilder.length() == 0;
    }

    /**
     * 字段类型
     */
    public enum FiledType {
        INT, STRING, BOOLEAN, DATETIME, DOUBLE, FLOAT, FUNCTION
    }
}
