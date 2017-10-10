package com.cloudzero.arch.database;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by lishiwu on 2016/10/12.
 */
public class DataStoreManagerTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        DataStoreManager.initConfig(ClassLoader.getSystemResource("node_db.xml").getFile());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        DataStoreManager.destroyAllDataSource();
    }

    @Test
    public void testInitConfig() throws Exception {
        DataStoreManager.initConfig(ClassLoader.getSystemResource("node_db.xml").getFile());
    }

    @Test
    public void testGetDataSource() throws Exception {
        String[] dataSourceIds = new String[]{"biz1", "biz2"};
        int dataSourceNum = 100;
        Random random = new Random();
        for (int i = 0; i < dataSourceNum; i++) {
            DataStoreManager.getConnection(dataSourceIds[random.nextInt(dataSourceIds.length)]);
        }
        System.out.println(DataStoreManager.getDataSoureIds());
    }

    @Test
    public void testRemoveDataSource() throws Exception {
        DataStoreManager.destroyDataSource("biz1");
        DataStoreManager.destroyDataSource("biz2");
    }

    @Test
    public void testGetConn() throws Exception {
        int loop = 10;
        Connection[] connections = new Connection[loop];
        for (int i = 0; i < loop; i++) {
            connections[i] = DataStoreManager.getConnection("biz1");
        }
        TimeUnit.SECONDS.sleep(10);
        for (int i = 0; i < loop; i++) {
            System.out.println(connections[i]);
        }
        TimeUnit.SECONDS.sleep(10);
        for (int i = 0; i < loop; i++) {
            connections[i].close();
        }
    }

    @Test
    public void testSelectNow() throws Exception {
        SQLExecutor sqlExecutor = DataStoreManager.getSQLExecutor("biz1");

        SQLStatement.Builder builder = SQLStatement.newBuilder();
        builder.append("select now()");
        System.out.println(sqlExecutor.queryMapList(builder.build()));
        SQLStatement.Builder builder2 = SQLStatement.newBuilder();
        builder2.append("select 1");
        System.out.println(sqlExecutor.queryMapList(builder2.build()));

        sqlExecutor.closeQuietly();

    }

    @Test
    public void testLockTable() throws Exception {
        SQLExecutor sqlExecutor = DataStoreManager.getSQLExecutor("biz1");

        SQLStatement.Builder builder = SQLStatement.newBuilder();
        builder.append("lock tables t_file_record read");

        sqlExecutor.closeQuietly();

    }
}