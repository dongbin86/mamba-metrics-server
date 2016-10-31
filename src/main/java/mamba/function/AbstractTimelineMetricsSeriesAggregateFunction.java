package mamba.function;

import com.google.common.base.Joiner;
import mamba.metrics.TimelineMetric;
import mamba.metrics.TimelineMetrics;

import java.util.*;

/**
 * Created by dongbin on 2016/10/10.
 */
public abstract class AbstractTimelineMetricsSeriesAggregateFunction
        implements TimelineMetricsSeriesAggregateFunction {

    @Override
    public TimelineMetric apply(TimelineMetrics timelineMetrics) {
        Set<String> metricNameSet = new TreeSet<>();
        Set<String> hostNameSet = new TreeSet<>();
        Set<String> appIdSet = new TreeSet<>();
        Set<String> instanceIdSet = new TreeSet<>();
        TreeMap<Long, List<Double>> metricValues = new TreeMap<>();

        for (TimelineMetric timelineMetric : timelineMetrics.getMetrics()) {
            metricNameSet.add(timelineMetric.getMetricName());
            addToSetOnlyNotNull(hostNameSet, timelineMetric.getHostName());
            addToSetOnlyNotNull(appIdSet, timelineMetric.getAppId());
            addToSetOnlyNotNull(instanceIdSet, timelineMetric.getInstanceId());

            for (Map.Entry<Long, Double> metricValue : timelineMetric.getMetricValues().entrySet()) {
                Long timestamp = metricValue.getKey();
                Double value = metricValue.getValue();
                if (!metricValues.containsKey(timestamp)) {
                    metricValues.put(timestamp, new LinkedList<Double>());
                }
                metricValues.get(timestamp).add(value);
            }
        }

        TreeMap<Long, Double> aggregatedMetricValues = new TreeMap<>();
        for (Map.Entry<Long, List<Double>> metricValue : metricValues.entrySet()) {
            List<Double> values = metricValue.getValue();
            if (values.size() == 0) {
                throw new IllegalArgumentException("count of values should be more than 0");
            }
            aggregatedMetricValues.put(metricValue.getKey(), applyFunction(values));
        }

        TimelineMetric timelineMetric = new TimelineMetric();
        timelineMetric.setMetricName(getMetricName(metricNameSet.iterator()));
        timelineMetric.setHostName(joinStringsWithComma(hostNameSet.iterator()));
        timelineMetric.setAppId(joinStringsWithComma(appIdSet.iterator()));
        timelineMetric.setInstanceId(joinStringsWithComma(instanceIdSet.iterator()));
        if (aggregatedMetricValues.size() > 0) {
            timelineMetric.setStartTime(aggregatedMetricValues.firstKey());
        }
        timelineMetric.setMetricValues(aggregatedMetricValues);
        return timelineMetric;
    }

    protected String getMetricName(Iterator<String> metricNames) {
        return getFunctionName() + "(" + Joiner.on(",").join(metricNames) + ")";
    }

    protected String joinStringsWithComma(Iterator<String> hostNames) {
        return Joiner.on(",").join(hostNames);
    }

    protected abstract Double applyFunction(List<Double> values);

    protected abstract String getFunctionName();

    private void addToSetOnlyNotNull(Set<String> set, String value) {
        if (value != null) {
            set.add(value);
        }
    }

}
