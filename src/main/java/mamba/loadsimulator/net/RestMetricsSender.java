package mamba.loadsimulator.net;


import com.google.common.base.Stopwatch;
import mamba.loadsimulator.util.Json;
import mamba.metrics.TimelineMetric;
import mamba.metrics.TimelineMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by sanbing on 10/10/16.
 */
public class RestMetricsSender implements MetricsSender {
    private final static Logger LOG = LoggerFactory.getLogger(RestMetricsSender.class);

    private final static String COLLECTOR_URL = "http://%s/ws/v1/timeline/metrics";
    private final String collectorServiceAddress;


    public RestMetricsSender(String metricsHost) {
        collectorServiceAddress = String.format(COLLECTOR_URL, metricsHost);
    }


    @Override
    public String pushMetrics(String payload) {
        String responseString = "";
        UrlService svc = null;
        Stopwatch timer = new Stopwatch().start();

        try {
            LOG.info("server: {}", collectorServiceAddress);

            System.out.println(collectorServiceAddress);

            System.out.println(payload);


            svc = getConnectedUrlService();
            responseString = svc.send(payload);

            timer.stop();
            LOG.info("http response time: " + timer.elapsedTime(TimeUnit.MILLISECONDS)
                    + " ms");

            if (responseString.length() > 0) {
                LOG.debug("POST response from server: " + responseString);
            }
        } catch (IOException e) {
            LOG.error("", e);
        } finally {
            if (svc != null) {
                svc.disconnect();
            }
        }

        return responseString;
    }

    /**
     * Relaxed to protected for testing.
     */
    protected UrlService getConnectedUrlService() throws IOException {
        return UrlService.newConnection(collectorServiceAddress);
    }


    public static void main(String[] args) throws IOException {
        TimelineMetrics metrics = new TimelineMetrics();
        TimelineMetric metric = new TimelineMetric();
        metric.setMetricName("swap_total");
        metric.setAppId("HOST");
        metric.setHostName("local0");
        metric.setInstanceId("");
        metric.setStartTime(1477767670892l);
        metric.setTimestamp(1477767670892l);
        metric.setType("typeA");
        metric.setUnits("MB");


        Map<Long, Double> values = new HashMap();
        values.put(1477767670892l,171.18301007673193d);
        metric.addMetricValues(values);
        List<TimelineMetric> metricsList = new ArrayList();

        metricsList.add(metric);
        metrics.setMetrics(metricsList);

        RestMetricsSender sender = new RestMetricsSender("127.0.0.1");

        String payload = new Json().serialize(metrics);
        sender.pushMetrics(payload);

        /*
        * private String metricName;
    private String appId;
    private String instanceId;
    private String hostName;
    private long timestamp;
    private long startTime;
    private String type;
    private String units;
    private TreeMap<Long, Double> metricValues = new TreeMap<Long, Double>();
        *
        * */


    }

}
