package com.cloudzero.arch.database;

/**
 * 包含总行数的结果集
 * Created by LiShiwu on 06/12/2017.
 */
public class PageData<T> {
    public int total;
    public T data;

    public PageData() {
    }

    public PageData(int total, T data) {
        this.total = total;
        this.data = data;
    }
}
