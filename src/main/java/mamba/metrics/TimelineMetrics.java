package mamba.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * @author dongbin  @Date: 10/28/16
 */
public class TimelineMetrics {

    private List<TimelineMetric> metrics = new ArrayList<TimelineMetric>();

    public TimelineMetrics() {
    }

    public List<TimelineMetric> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<TimelineMetric> metrics) {
        this.metrics = metrics;
    }

    private boolean isEqualTimelineMetrics(TimelineMetric metric1,
                                           TimelineMetric metric2) {

        boolean isEqual = true;

        if (!metric1.getMetricName().equals(metric2.getMetricName())) {
            return false;
        }

        if (metric1.getHostName() != null) {
            isEqual = metric1.getHostName().equals(metric2.getHostName());
        }

        if (metric1.getAppId() != null) {
            isEqual = metric1.getAppId().equals(metric2.getAppId());
        }

        return isEqual;
    }


    public void addOrMergeTimelineMetric(TimelineMetric metric) {
        TimelineMetric metricToMerge = null;

        if (!metrics.isEmpty()) {
            for (TimelineMetric timelineMetric : metrics) {
                if (timelineMetric.equalsExceptTime(metric)) {
                    metricToMerge = timelineMetric;
                    break;
                }
            }
        }

        if (metricToMerge != null) {
            metricToMerge.addMetricValues(metric.getMetricValues());
            if (metricToMerge.getTimestamp() > metric.getTimestamp()) {
                metricToMerge.setTimestamp(metric.getTimestamp());
            }
            if (metricToMerge.getStartTime() > metric.getStartTime()) {
                metricToMerge.setStartTime(metric.getStartTime());
            }
        } else {
            metrics.add(metric);
        }
    }

    public void addOrMergeTimelineMetric(SingleValuedTimelineMetric metric) {
        TimelineMetric metricToMerge = null;

        if (!metrics.isEmpty()) {
            for (TimelineMetric timelineMetric : metrics) {
                if (metric.equalsExceptTime(timelineMetric)) {
                    metricToMerge = timelineMetric;
                    break;
                }
            }
        }

        if (metricToMerge != null) {
            metricToMerge.getMetricValues().put(metric.getTimestamp(), metric.getValue());
            if (metricToMerge.getTimestamp() > metric.getTimestamp()) {
                metricToMerge.setTimestamp(metric.getTimestamp());
            }
            if (metricToMerge.getStartTime() > metric.getStartTime()) {
                metricToMerge.setStartTime(metric.getStartTime());
            }
        } else {
            metrics.add(metric.getTimelineMetric());
        }
    }

    @Override
    public String toString() {
        return "TimelineMetrics{" +
                "allMetrics=" + metrics +
                '}';
    }


    public static void main(String[] args) {
        TimelineMetrics timelineMetrics = new TimelineMetrics();
        List<TimelineMetric> metricList = new ArrayList<TimelineMetric>();
        timelineMetrics.setMetrics(metricList);
        TimelineMetric metric1 = new TimelineMetric();
        TimelineMetric metric2 = new TimelineMetric();
        metricList.add(metric1);
        metricList.add(metric2);
        metric1.setMetricName("cpu_user");
        metric1.setAppId("1");
        metric1.setInstanceId(null);
        metric1.setHostName("c6401");
        metric1.setStartTime(1407949812L);
        metric1.setMetricValues(new TreeMap<Long, Double>() {{
            put(1407949812L, 1.0d);
            put(1407949912L, 1.8d);
            put(1407950002L, 0.7d);
        }});

        metric2.setMetricName("mem_free");
        metric2.setAppId("2");
        metric2.setInstanceId("3");
        metric2.setHostName("c6401");
        metric2.setStartTime(1407949812L);
        metric2.setMetricValues(new TreeMap<Long, Double>() {{
            put(1407949812L, 2.5d);
            put(1407949912L, 3.0d);
            put(1407950002L, 0.9d);
        }});
        ObjectMapper mapper = new ObjectMapper();
        Gson gson = new Gson();

        System.out.println(gson.toJson(timelineMetrics));


        String w = "{\"metrics\":[{\"metricName\":\"cpu_user\",\"appId\":\"1\",\"hostName\":\"c6401\",\"timestamp\":0,\"startTime\":1407949812,\"metricValues\":{\"1407949812\":1.0,\"1407949912\":1.8,\"1407950002\":0.7}},{\"metricName\":\"mem_free\",\"appId\":\"2\",\"instanceId\":\"3\",\"hostName\":\"c6401\",\"timestamp\":0,\"startTime\":1407949812,\"metricValues\":{\"1407949812\":2.5,\"1407949912\":3.0,\"1407950002\":0.9}}]}\n";
        TimelineMetrics metrics = gson.fromJson(w, TimelineMetrics.class);
        System.out.println(metrics);
        try {
            TimelineMetrics m = mapper.readValue(w, TimelineMetrics.class);
            System.out.println("made!!!!");
            System.out.println(m);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
