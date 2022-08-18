package com.github.cysong.dbassert.datasource;

import com.github.cysong.dbassert.exception.ConfigurationException;
import com.github.cysong.dbassert.utitls.Utils;
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
import java.util.logging.Logger;

public class DefaultConnectionFactory implements ConnectionFactory {
    private static final Logger log = Logger.getLogger(DefaultConnectionFactory.class.getName());
    private String databaseFile;
    private volatile boolean inited = false;
    private Map<String, DatabaseConfig> configMap;
    private Map<String, Connection> conns;

    public DefaultConnectionFactory(String databaseFile) {
        this.databaseFile = databaseFile;
    }

    @Override
    public Connection getConnectionByDbKey(String dbId) {
        assert dbId != null;
        if (!inited) {
            parseDatabaseConfig();
        }
        if (!(conns != null && conns.containsKey(dbId))) {
            initConnectionByDbId(dbId);
        }
        Connection conn = conns.get(dbId);
        if (conn == null) {
            throw new ConfigurationException("Database config not found：" + dbId);
        }
        if (isConnValid(conn)) {
            return conn;
        } else {
            log.info(String.format("Database[%s] connect is not valid, reconnect", dbId));
            conns.remove(dbId);
            initConnectionByDbId(dbId);
            conn = conns.get(dbId);
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
            assert config.getId() != null && config.getUrl() != null;
            configMap.put(config.getId(), config);
        }
        inited = true;
    }

    private void initConnectionByDbId(String dbId) {
        synchronized (dbId) {
            if (conns != null && conns.containsKey(dbId)) {
                return;
            }
            DatabaseConfig dbConf = configMap.get(dbId);
            if (dbConf == null) {
                throw new ConfigurationException(String.format("Database config of id %s not found", dbId));
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
                        throw new ConfigurationException(String.format("Connect to database[%s] fail：%s,%s", dbId, dbConf.getUrl(), dbConf.getUsername()), e);
                    } else {
                        log.info(String.format("The %s times connect to database[{%s}] fail, retry", (3 - count), dbId));
                    }
                } catch (SQLException e) {
                    throw new ConfigurationException(String.format("database [%s] connect fail：%s,%s", dbId, dbConf.getUrl(), dbConf.getUsername()), e);
                }
            }
            log.info(String.format("Database[%s] connect success：%s,%s", dbId, dbConf.getUrl(), dbConf.getUsername()));
            if (conns == null) {
                conns = new ConcurrentHashMap<>();
            }
            conns.put(dbId, connection);
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
        for (Map.Entry<String, Connection> entry : conns.entrySet()) {
            if (entry.getValue() != null) {
                log.info("Database " + entry.getKey() + " is closing...");
                try {
                    entry.getValue().close();
                } catch (SQLException e) {

                }
            }
        }
        configMap.clear();
        conns.clear();
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
