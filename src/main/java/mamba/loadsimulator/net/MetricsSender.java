package mamba.loadsimulator.net;

/**
 * Created by sanbing on 10/10/16.
 */
public interface MetricsSender {
    String pushMetrics(String payload);
}
