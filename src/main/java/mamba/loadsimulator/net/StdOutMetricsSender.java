package mamba.loadsimulator.net;

import java.io.PrintStream;

/**
 * Created by sanbing on 10/10/16.
 */
public class StdOutMetricsSender implements MetricsSender {
    public final PrintStream out;
    private String metricsHostName;

    /**
     * Creates new StdOutMetricsSender with specified hostname (only used in messages) and sends output to System.out
     *
     * @param metricsHostName a name used in printed messages
     */
    public StdOutMetricsSender(String metricsHostName) {
        this(metricsHostName, System.out);
    }

    /**
     * Creates new StdOutMetricsSender with specified hostname (only used in messages) and PrintStream which is used as
     * an output.
     *
     * @param metricsHostName a name used in printed messages
     * @param out             PrintStream that the Sender will write to, can be System.out
     */
    public StdOutMetricsSender(String metricsHostName, PrintStream out) {
        this.metricsHostName = metricsHostName;
        this.out = out;
    }

    @Override
    public String pushMetrics(String payload) {
        out.println("Sending to " + metricsHostName + ": " + payload);

        return "OK";
    }
}
