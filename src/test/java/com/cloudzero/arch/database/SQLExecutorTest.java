package com.cloudzero.arch.database;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by lishiwu on 2016/8/12.
 */
public class SQLExecutorTest {
    private static SQLExecutor SQLExecutor;

    @BeforeClass
    public static void setUp() throws Exception {
        SQLExecutor = ConnUtil.getTXExecutor("com.mysql.cj.jdbc.Driver", "jdbc:mysql://127.0.0.1:3306/testdb?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true&useSSL=false&serverTimezone=GMT%2b8&generateSimpleParameterMetadata=true", "user", "user");
    }

    @Test
    public void queryMapList() throws Exception {
        SQLStatement.Builder builder = SQLStatement.newBuilder();
        builder.append("select * from t_profit_record").append(" where fund_code = ? ", "001038")
                .append(" and amount between ? and ? ", 0, 300).append(true, " order by id desc", null).limitOne();
        System.out.println(SQLExecutor.queryMapList(builder.build()));

    }

    @Test
    public void queryList() throws Exception {
        SQLStatement.Builder builder = SQLStatement.newBuilder();
        builder.append("select * from t_profit_record").append(" where fund_code = ? ", "001038")
                .append(" and create_time < ? ", new Date()).append(true, " order by id desc", null).limitOne();
        System.out.println(SQLExecutor.queryMapList(builder.build()));
    }

    @Test
    public void batchSave() throws Exception {

    }

    @Test
    public void saveAndGetKey() throws Exception {
        SQLStatement.Builder builder = SQLStatement.newBuilder();
        builder.append("INSERT INTO `testdb`.`t_receipt_record`" +
                " (`id`,`request_no`,`trans_date`,`trading_type`,`amount`,`task_status`,`pre1`,`pre2`,`creat_time`) values")
                .append(" (null,?,?,?,?,?,?,?,?)", UUID.randomUUID().toString(), "20101010", "INVEST", 222, "FAIELD and 1=1", "pre1", "pre2", new Date());
        System.out.println(SQLExecutor.persistAndGetKey(builder.build()));
        SQLExecutor.commitAndClose();
    }

    @Test
    public void saveOrUpdate() throws Exception {

    }

    @Test
    public void multiSaveOrUpdate() throws Exception {
        List<SQLStatement> list = new ArrayList<>();
        SQLStatement.Builder builder = SQLStatement.newBuilder();
        builder.append("INSERT INTO `testdb`.`t_receipt_record`" +
                " (`id`,`request_no`,`trans_date`,`trading_type`,`amount`,`task_status`,`pre1`,`pre2`,`creat_time`) values")
                .append(" (3,?,?,?,?,?,?,?,?)", UUID.randomUUID().toString(), "20101010", "INVEST", 222, "FAIELD and 1=1", "pre1", "pre2", new Date());
        list.add(builder.build());
        builder.clear();
        builder.append("INSERT INTO `testdb`.`t_receipt_record`" +
                " (`id`,`request_no`,`trans_date`,`trading_type`,`amount`,`task_status`,`pre1`,`pre2`,`creat_time`) values")
                .append(" (4,?,?,?,?,?,?,?,now())", UUID.randomUUID().toString(), "20101010", "INVEST", 222, "FAIELD and 1=1", "pre1", "pre2");
        list.add(builder.build());
        SQLExecutor.persistOrUpdate(list);
        SQLExecutor.commitAndClose();
    }

    @Test
    public void tt1() throws Exception {
        SQLStatement.Builder cdnBuilder = SQLStatement.newBuilder();
        cdnBuilder.append(" where 1=1");
        SQLStatement.Builder sltbuilder = SQLStatement.newBuilder();
        sltbuilder.append("select * from dual");
        sltbuilder.append(cdnBuilder.build());
        System.out.println(sltbuilder.build());
    }

    @Test
    public void tt2() throws Exception {
        SQLStatement.Builder sltbuilder = SQLStatement.newBuilder();
        sltbuilder.append("select * from dual where 1=?", 1).append(" and 4=?", 4);
        SQLStatement.Builder cdnBuilder = SQLStatement.newBuilder();
        cdnBuilder.append(" and 2=?", 2).append(" and 3=?", 3).prepend(sltbuilder.build());
        System.out.println(cdnBuilder.build());
    }

    @Test
    public void tt3() throws Exception {
        SQLStatement.Builder builder = SQLStatement.newBuilder();
        builder.prepend("SELECT * FROM t_third_user_account where id_no like '13%'").limit(5, 5);
        PageData pd = SQLExecutor.queryPageMapList(builder.build());
        System.out.println("total:"+pd.total);
        System.out.println("data:"+pd.data);
    }
    @Test
    public void tt4() throws Exception {
        SQLStatement.Builder builder = SQLStatement.newBuilder();
        builder.prepend("SELECT id_no FROM t_third_user_account where id_no like '13%'").limit(5, 5);
        PageData pd = SQLExecutor.queryPageList(builder.build(), SimpleResult.StringResult);
        System.out.println("total:"+pd.total);
        System.out.println("data:"+pd.data);
    }
}