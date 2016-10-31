package mamba.loadsimulator.data;

/**
 * Created by sanbing on 10/10/16.
 */
public enum AppID {
    HOST("HOST"),
    NAMENODE("namenode"),
    RESOURCEMANAGER("resourcemanager"),
    DATANODE("datanode"),
    NODEMANAGER("nodemanager"),
    MASTER_HBASE("hbase"),
    SLAVE_HBASE("hbase"),
    NIMBUS("nimbus"),
    KAFKA_BROKER("kafka_broker");

    public static final AppID[] MASTER_APPS = {HOST, NAMENODE, RESOURCEMANAGER, MASTER_HBASE, KAFKA_BROKER, NIMBUS};
    public static final AppID[] SLAVE_APPS = {HOST, DATANODE, NODEMANAGER, SLAVE_HBASE};

    private String id;

    private AppID(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

}
