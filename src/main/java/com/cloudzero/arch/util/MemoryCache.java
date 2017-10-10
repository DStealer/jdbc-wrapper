package com.cloudzero.arch.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 临时存储对象容器 先 set 后 get
 * Created by lishiwu on 2016/7/27.
 */
public class MemoryCache<K, V> {
    private ConcurrentMap<K, V> cache = new ConcurrentHashMap<>(512);
    private volatile boolean locked = false;

    /**
     * 获取
     *
     * @param key
     * @return
     */
    public V get(K key) {
        return this.cache.get(key);
    }

    /**
     * 获取
     *
     * @param key
     * @return
     */
    public V getOrDefault(K key, V defaultValue) {
        V v = this.cache.get(key);
        return v != null ? v : defaultValue;
    }

    /**
     * 添加
     *
     * @param key
     * @param value
     * @return
     */
    public synchronized MemoryCache<K, V> set(K key, V value) {
        if (!locked) {
            this.cache.put(key, value);
        } else {
            throw new RuntimeException("Cache is locked");
        }
        return this;
    }

    /**
     * 添加
     *
     * @param key
     * @param value
     * @return
     */
    public synchronized MemoryCache<K, V> setIfAbsent(K key, V value) {
        if (!locked) {
            this.cache.putIfAbsent(key, value);
        } else {
            throw new RuntimeException("Cache is locked");
        }
        return this;
    }

    /**
     * 添加多个
     *
     * @param m
     * @return
     */
    public synchronized MemoryCache<K, V> setAll(Map<K, V> m) {
        if (!locked) {
            this.cache.putAll(m);
        } else {
            throw new RuntimeException("Cache is locked");
        }
        return this;
    }

    /**
     * 清除所有元素并重置初始化标志
     */
    public synchronized void clear() {
        locked = false;
        this.cache.clear();
    }

    /**
     * 锁定，无法修改
     */
    public synchronized void lock() {
        locked = true;
    }

    /**
     * 解锁
     */
    public synchronized void unlock() {
        locked = false;
    }
}
