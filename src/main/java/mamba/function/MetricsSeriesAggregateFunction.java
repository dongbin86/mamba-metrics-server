package mamba.function;

import mamba.metrics.Metric;
import mamba.metrics.Metrics;

/**
 * Created by dongbin on 2016/10/10.
 */
public interface MetricsSeriesAggregateFunction {
    Metric apply(Metrics metrics);
}
