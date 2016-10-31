package mamba.loadsimulator.data;

/**
 * Created by sanbing on 10/10/16.
 */
public final class ApplicationInstance {

    private final transient String hostName;
    private final transient AppID appId;
    private final transient String instanceId;

    /**
     * @param hostname
     * @param appId
     * @param instanceId
     */
    public ApplicationInstance(String hostname, AppID appId, String instanceId) {
        if (hostname == null || appId == null || instanceId == null)
            throw new IllegalArgumentException("ApplicationInstance can not be " +
                    "instantiated with null values");

        this.hostName = hostname;
        this.appId = appId;
        this.instanceId = instanceId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public AppID getAppId() {
        return appId;
    }

    public String getHostName() {
        return hostName;
    }


}
