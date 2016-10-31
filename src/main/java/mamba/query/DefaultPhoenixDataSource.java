package mamba.query;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by dongbin on 2016/10/10.
 */
public class DefaultPhoenixDataSource implements PhoenixConnectionProvider {

    static final Log LOG = LogFactory.getLog(DefaultPhoenixDataSource.class);
    private static final String ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    private static final String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final String ZNODE_PARENT = "zookeeper.znode.parent";

    private static final String connectionUrl = "jdbc:phoenix:%s:%s:%s";
    private final String url;

    private Configuration hbaseConf;

    public DefaultPhoenixDataSource(Configuration hbaseConf) {
        this.hbaseConf = hbaseConf;
        String zookeeperClientPort = hbaseConf.getTrimmed(ZOOKEEPER_CLIENT_PORT, "2181");
        String zookeeperQuorum = hbaseConf.getTrimmed(ZOOKEEPER_QUORUM);
        String znodeParent = hbaseConf.getTrimmed(ZNODE_PARENT, "/ams-hbase-unsecure");
        if (zookeeperQuorum == null || zookeeperQuorum.isEmpty()) {
            throw new IllegalStateException("Unable to find Zookeeper quorum to " +
                    "access HBase store using Phoenix.");
        }

        url = String.format(connectionUrl,
                zookeeperQuorum,
                zookeeperClientPort,
                znodeParent);
    }


    public HBaseAdmin getHBaseAdmin() throws IOException {
        return (HBaseAdmin) ConnectionFactory.createConnection(hbaseConf).getAdmin();
    }


    public Connection getConnection() throws SQLException {

        LOG.debug("Metric store connection url: " + url);
        try {
            return DriverManager.getConnection(url);
        } catch (SQLException e) {
            LOG.warn("Unable to connect to HBase store using Phoenix.", e);

            throw e;
        }
    }

}
