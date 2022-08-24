package com.github.cysong.dbassert.datasource;

import com.github.cysong.dbassert.exception.ConfigurationException;
import com.github.cysong.dbassert.utitls.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.SequenceNode;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * default value if ConnectionFactory if not set
 * {@link com.github.cysong.dbassert.option.DbAssertOptions#factory(ConnectionFactory factory)}
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class DefaultConnectionFactory implements ConnectionFactory {
    private static final Logger log = LoggerFactory.getLogger(DefaultConnectionFactory.class);
    private String databaseFile;
    private volatile boolean inited = false;
    private Map<String, DatabaseConfig> configMap;
    private Map<String, Connection> connMap;

    public DefaultConnectionFactory(String databaseFile) {
        this.databaseFile = databaseFile;
    }

    @Override
    public Connection getConnectionByDbKey(String dbKey) {
        assert dbKey != null;
        if (!inited) {
            parseDatabaseConfig();
        }
        if (!(connMap != null && connMap.containsKey(dbKey))) {
            initConnectionByDbKey(dbKey);
        }
        Connection conn = connMap.get(dbKey);
        if (conn == null) {
            throw new ConfigurationException("Database config not found：" + dbKey);
        }
        if (isConnValid(conn)) {
            return conn;
        } else {
            log.info("Database[{}] connection is not valid, reconnect...", dbKey);
            connMap.remove(dbKey);
            initConnectionByDbKey(dbKey);
            conn = connMap.get(dbKey);
        }
        return conn;
    }

    private synchronized void parseDatabaseConfig() {
        if (inited) {
            return;
        }
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(databaseFile);
        if (is == null) {
            throw new ConfigurationException("Database config file not found:" + databaseFile);
        }
        Yaml yaml = new Yaml(new ListConstructor<>(DatabaseConfig.class));
        List<DatabaseConfig> configList = yaml.load(is);
        if (configList.size() == 0) {
            throw new ConfigurationException("Database config file is empty:" + databaseFile);
        }
        configMap = new ConcurrentHashMap<>(configList.size());
        for (DatabaseConfig config : configList) {
            assert config.getKey() != null && config.getUrl() != null;
            configMap.put(config.getKey(), config);
        }
        inited = true;
    }

    private void initConnectionByDbKey(String dbKey) {
        synchronized (dbKey) {
            if (connMap != null && connMap.containsKey(dbKey)) {
                return;
            }
            DatabaseConfig dbConf = configMap.get(dbKey);
            if (dbConf == null) {
                throw new ConfigurationException(String.format("Database config of key=%s not found", dbKey));
            }

            if (Utils.isNotBlank(dbConf.getDriver())) {
                try {
                    Class.forName(dbConf.getDriver());
                } catch (ClassNotFoundException e) {
                    throw new ConfigurationException("Database driver class not found:" + dbConf.getDriver(), e);
                }
            }
            Connection connection = null;
            //retry 3 times if connect fail
            int count = 3;
            DriverManager.setLoginTimeout(30);
            while (count-- > 0) {
                try {
                    connection = DriverManager.getConnection(dbConf.getUrl(), dbConf.getUsername(), dbConf.getPassword());
                    break;
                } catch (SQLTimeoutException e) {
                    if (count == 0) {
                        throw new ConfigurationException(String.format("Connect to database[%s] fail：%s,%s", dbKey, dbConf.getUrl(), dbConf.getUsername()), e);
                    } else {
                        log.info("The {} times connect to database[{}] fail, retry", (3 - count), dbKey);
                    }
                } catch (SQLException e) {
                    throw new ConfigurationException(String.format("database [%s] connect fail：%s,%s", dbKey, dbConf.getUrl(), dbConf.getUsername()), e);
                }
            }
            log.info("Database[{}] connect success：{},{}", dbKey, dbConf.getUrl(), dbConf.getUsername());
            if (connMap == null) {
                connMap = new ConcurrentHashMap<>(configMap.size());
            }
            connMap.put(dbKey, connection);
        }
    }

    private static boolean isConnValid(Connection conn) {
        try {
            return conn.isValid(10);
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void destroy() {
        if (connMap != null && connMap.size() > 0) {
            for (Map.Entry<String, Connection> entry : connMap.entrySet()) {
                if (entry.getValue() != null) {
                    log.info("Database {} is closing...", entry.getKey());
                    try {
                        entry.getValue().close();
                    } catch (SQLException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
            connMap.clear();
        }
        if (configMap != null && configMap.size() > 0) {
            configMap.clear();
        }
    }


    public class ListConstructor<T> extends Constructor {
        private final Class<T> clazz;

        public ListConstructor(final Class<T> clazz) {
            this.clazz = clazz;
        }

        @Override
        protected Object constructObject(final Node node) {
            if (node instanceof SequenceNode && isRootNode(node)) {
                ((SequenceNode) node).setListType(clazz);
            }
            return super.constructObject(node);
        }

        private boolean isRootNode(final Node node) {
            return node.getStartMark().getIndex() == 0;
        }
    }

}
