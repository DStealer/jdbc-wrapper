package com.cloudzero.arch.database;

import com.alibaba.druid.pool.DruidDataSource;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by lishiwu on 2016/10/12.
 */
public final class DataStoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreManager.class);
    private static final ConcurrentMap<String, DruidDataSource> DATA_SOURCE_MANAGER = new ConcurrentHashMap<>(4, 0.75f);
    private static String xmlConfigFilePath = "";

    /**
     * 根据xml配置初始化数据库连接池
     *
     * @param xmlConfigFilePath
     * @throws Exception
     */
    public static synchronized void initConfig(String xmlConfigFilePath) throws Exception {
        DataStoreManager.xmlConfigFilePath = xmlConfigFilePath;
        SAXReader saxReader = new SAXReader();
        Document document = saxReader.read(new FileInputStream(DataStoreManager.xmlConfigFilePath));
        List<Element> druidElementList = document.selectNodes("//datastore/druid[@id]");
        if (druidElementList.size() > 0) {
            for (Element druidElement : druidElementList) {
                DATA_SOURCE_MANAGER.computeIfAbsent(druidElement.attributeValue("id").trim(), dis -> createDataSourceWithCfg(druidElement));
            }
        } else {
            LOGGER.warn("Element[<datastore><druid>**</druid></datastore>] no found");
        }
    }

    /**
     * 从DATA_SOURCE_MANAGER中销毁数据源
     *
     * @param dataSourceId
     */
    public static void destroyDataSource(String dataSourceId) {
        DruidDataSource druidDataSource = DATA_SOURCE_MANAGER.remove(dataSourceId);
        if (druidDataSource != null) {
            druidDataSource.close();
        }
        LOGGER.debug("Destroy datasource :" + dataSourceId + " success");
    }

    /**
     * 从DATA_SOURCE_MANAGER中销毁所有数据源
     */
    public static void destroyAllDataSource() {
        DATA_SOURCE_MANAGER.values().forEach(dds -> dds.close());
        DATA_SOURCE_MANAGER.clear();
        LOGGER.debug("Destroy all datasource success");
    }

    /**
     * 获取Connection(ReadOnly)
     *
     * @param dataSourceId
     * @return
     */
    public static Connection getConnection(String dataSourceId) throws Exception {
        Connection connection = DATA_SOURCE_MANAGER.get(dataSourceId).getConnection();
        connection.setReadOnly(true);
        return connection;
    }

    /**
     * 获取SQLExecutor(ReadOnly)
     *
     * @param dataSourceId
     * @return
     */
    public static SQLExecutor getSQLExecutor(String dataSourceId) throws Exception {
        Connection connection = DATA_SOURCE_MANAGER.get(dataSourceId).getConnection();
        connection.setReadOnly(true);
        return new SQLExecutor(connection);
    }

    /**
     * 获取数据库连接(开启事务)
     *
     * @param dataSourceId
     * @return
     */
    public static Connection getTxConnection(String dataSourceId) throws Exception {
        Connection connection = DATA_SOURCE_MANAGER.get(dataSourceId).getConnection();
        connection.setAutoCommit(true);
        return DATA_SOURCE_MANAGER.get(dataSourceId).getConnection();
    }

    /**
     * 获取数据库连接(开启事务)
     *
     * @param dataSourceId
     * @return
     */
    public static SQLExecutor getTxSQLExecutor(String dataSourceId) throws Exception {
        Connection connection = DATA_SOURCE_MANAGER.get(dataSourceId).getConnection();
        connection.setAutoCommit(true);
        return new SQLExecutor(connection);
    }

    /**
     * 获取管理中容器中的DataSourceId
     *
     * @return
     */
    public static Set<String> getDataSoureIds() {
        return DATA_SOURCE_MANAGER.keySet();
    }

    /**
     * 根据xml的配置初始化数据源
     *
     * @param druidElement
     * @return
     */
    private static DruidDataSource createDataSourceWithCfg(Element druidElement) {
        LOGGER.info("Try to load DataSource :" + druidElement.attributeValue("id").trim());
        if (!Boolean.valueOf(druidElement.attributeValue("enable"))) {
            LOGGER.warn("DataSource :" + druidElement.attributeValue("id").trim() + " is disable");
            return null;
        }
        DruidDataSource druidDataSource = new DruidDataSource();

        String url = selectNodeValByNameAttr(druidElement, "url");
        if (url != null && url.length() > 0) {
            druidDataSource.setUrl(url);
        } else {
            throw new InvalidParameterException("url can't be null");
        }
        String username = selectNodeValByNameAttr(druidElement, "username");
        if (username != null && username.length() > 0) {
            druidDataSource.setUsername(username);
        } else {
            throw new InvalidParameterException("username can't be null");
        }
        String password = selectNodeValByNameAttr(druidElement, "password");
        if (password != null && password.length() > 0) {
            druidDataSource.setPassword(password);
        } else {
            throw new InvalidParameterException("password can't be null");
        }
        try {
            druidDataSource.setDriverClassName(selectNodeValByNameAttr(druidElement, "driverClassName"));
        } catch (Exception e) {
            LOGGER.debug("driverClassName can't determine");
        }
        try {
            druidDataSource.setInitialSize(Integer.valueOf(selectNodeValByNameAttr(druidElement, "initialSize")));
        } catch (Exception e) {
            LOGGER.debug("initialSize can't determine");
        }
        try {
            druidDataSource.setMinIdle(Integer.valueOf(selectNodeValByNameAttr(druidElement, "minIdle")));
        } catch (Exception e) {
            LOGGER.debug("minIdle can't determine");
        }
        try {
            druidDataSource.setMaxActive(Integer.valueOf(selectNodeValByNameAttr(druidElement, "maxActive")));
        } catch (Exception e) {
            LOGGER.debug("maxActive can't determine");
        }
        try {
            druidDataSource.setMaxWait(Integer.valueOf(selectNodeValByNameAttr(druidElement, "maxWait")));
        } catch (Exception e) {
            LOGGER.debug("maxWait can't determine");
        }
        try {
            druidDataSource.setTimeBetweenEvictionRunsMillis(Integer.valueOf(selectNodeValByNameAttr(druidElement, "timeBetweenEvictionRunsMillis")));
        } catch (Exception e) {
            LOGGER.debug("timeBetweenEvictionRunsMillis can't determine");
        }
        try {
            druidDataSource.setMinEvictableIdleTimeMillis((Integer.valueOf(selectNodeValByNameAttr(druidElement, "minEvictableIdleTimeMillis"))));
        } catch (Exception e) {
            LOGGER.debug("minEvictableIdleTimeMillis can't determine");
        }
        try {
            druidDataSource.setValidationQuery(selectNodeValByNameAttr(druidElement, "validationQuery"));
        } catch (Exception e) {
            LOGGER.debug("validationQuery can't determine");
        }
        try {
            druidDataSource.setTestWhileIdle((Boolean.valueOf(selectNodeValByNameAttr(druidElement, "testWhileIdle"))));
        } catch (Exception e) {
            LOGGER.debug("testWhileIdle can't determine");
        }
        try {
            druidDataSource.setTestOnBorrow(Boolean.valueOf(selectNodeValByNameAttr(druidElement, "testOnBorrow")));
        } catch (Exception e) {
            LOGGER.debug("testOnBorrow can't determine");
        }
        try {
            druidDataSource.setTestOnReturn(Boolean.valueOf(selectNodeValByNameAttr(druidElement, "testOnReturn")));
        } catch (Exception e) {
            LOGGER.debug("testOnReturn can't determine");
        }
        try {
            druidDataSource.setPoolPreparedStatements(Boolean.valueOf(selectNodeValByNameAttr(druidElement, "poolPreparedStatements")));
        } catch (Exception e) {
            LOGGER.debug("poolPreparedStatements can't determine");
        }
        try {
            druidDataSource.setMaxPoolPreparedStatementPerConnectionSize(Integer.valueOf(selectNodeValByNameAttr(druidElement, "maxPoolPreparedStatementPerConnection")));
        } catch (Exception e) {
            LOGGER.debug("maxPoolPreparedStatementPerConnection can't determine");
        }
        try {
            druidDataSource.setFilters(selectNodeValByNameAttr(druidElement, "filters"));
        } catch (Exception e) {
            LOGGER.debug("filters can't determine");
        }
        try {
            druidDataSource.setFilters(selectNodeValByNameAttr(druidElement, "filters"));
        } catch (Exception e) {
            LOGGER.debug("filters can't determine");
        }
        try {
            druidDataSource.init();
            LOGGER.info("DataSource :" + druidElement.attributeValue("id").trim() + " loads successfully");
            return druidDataSource;
        } catch (SQLException e) {
            LOGGER.warn("DataSource :" + druidElement.attributeValue("id").trim() + " fails to load", e);
        }
        return null;
    }

    /**
     * 选择 property 元素且这些元素拥有值为 attrName 的 name 属性的，返回其value属性
     *
     * @param parent
     * @param attrName
     * @return
     */
    private static String selectNodeValByNameAttr(Element parent, String attrName) {
        Element e = (Element) parent.selectSingleNode("property[@name='" + attrName + "']");
        if (e != null) {
            if (e.attributeValue("value") != null) {
                return e.attributeValue("value").trim();
            } else {
                return e.getStringValue().trim();
            }
        } else {
            throw new InvalidParameterException("Can't find <property> with attribute: name = " + attrName);
        }
    }
}
