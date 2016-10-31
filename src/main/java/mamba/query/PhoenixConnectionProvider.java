package mamba.query;

import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;


/**
 * Created by dongbin on 2016/10/10.
 */
public interface PhoenixConnectionProvider extends ConnectionProvider {

    HBaseAdmin getHBaseAdmin() throws IOException;
}
