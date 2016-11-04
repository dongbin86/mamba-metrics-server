package mamba.function;

import com.google.common.base.Joiner;
import mamba.metrics.Metric;
import mamba.metrics.Metrics;

import java.util.*;

/**
 * Created by dongbin on 2016/10/10.
 */
public abstract class AbstractMetricsSeriesAggregateFunction
        implements MetricsSeriesAggregateFunction {

    @Override
    public Metric apply(Metrics metrics) {
        Set<String> metricNameSet = new TreeSet<>();
        Set<String> hostNameSet = new TreeSet<>();
        Set<String> appIdSet = new TreeSet<>();
        Set<String> instanceIdSet = new TreeSet<>();
        TreeMap<Long, List<Double>> metricValues = new TreeMap<>();

        for (Metric metric : metrics.getMetrics()) {
            metricNameSet.add(metric.getMetricName());
            addToSetOnlyNotNull(hostNameSet, metric.getHostName());
            addToSetOnlyNotNull(appIdSet, metric.getAppId());
            addToSetOnlyNotNull(instanceIdSet, metric.getInstanceId());

            for (Map.Entry<Long, Double> metricValue : metric.getMetricValues().entrySet()) {
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

        Metric metric = new Metric();
        metric.setMetricName(getMetricName(metricNameSet.iterator()));
        metric.setHostName(joinStringsWithComma(hostNameSet.iterator()));
        metric.setAppId(joinStringsWithComma(appIdSet.iterator()));
        metric.setInstanceId(joinStringsWithComma(instanceIdSet.iterator()));
        if (aggregatedMetricValues.size() > 0) {
            metric.setStartTime(aggregatedMetricValues.firstKey());
        }
        metric.setMetricValues(aggregatedMetricValues);
        return metric;
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
