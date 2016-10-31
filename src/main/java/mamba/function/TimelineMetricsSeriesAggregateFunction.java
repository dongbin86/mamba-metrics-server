package mamba.function;

import mamba.metrics.TimelineMetric;
import mamba.metrics.TimelineMetrics;

/**
 * Created by dongbin on 2016/10/10.
 */
public interface TimelineMetricsSeriesAggregateFunction {
    TimelineMetric apply(TimelineMetrics timelineMetrics);
}
