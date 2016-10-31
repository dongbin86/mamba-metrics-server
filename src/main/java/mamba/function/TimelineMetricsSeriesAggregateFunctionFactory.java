package mamba.function;

import mamba.aggregators.Function;

/**
 * Created by dongbin on 2016/10/10.
 */
public class TimelineMetricsSeriesAggregateFunctionFactory {
    private TimelineMetricsSeriesAggregateFunctionFactory() {
    }

    public static TimelineMetricsSeriesAggregateFunction newInstance(SeriesAggregateFunction func) {
        switch (func) {
            case AVG:
                return new TimelineMetricsSeriesAvgAggregateFunction();
            case MIN:
                return new TimelineMetricsSeriesMinAggregateFunction();
            case MAX:
                return new TimelineMetricsSeriesMaxAggregateFunction();
            case SUM:
                return new TimelineMetricsSeriesSumAggregateFunction();
            default:
                throw new Function.FunctionFormatException("Function should be sum, avg, min, max. Got " +
                        func.name());
        }
    }
}