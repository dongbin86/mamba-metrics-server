package mamba.function;

import mamba.aggregators.Function;

/**
 * Created by dongbin on 2016/10/10.
 */
public class MetricsSeriesAggregateFunctionFactory {
    private MetricsSeriesAggregateFunctionFactory() {
    }

    public static MetricsSeriesAggregateFunction newInstance(SeriesAggregateFunction func) {
        switch (func) {
            case AVG:
                return new MetricsSeriesAvgAggregateFunction();
            case MIN:
                return new MetricsSeriesMinAggregateFunction();
            case MAX:
                return new MetricsSeriesMaxAggregateFunction();
            case SUM:
                return new MetricsSeriesSumAggregateFunction();
            default:
                throw new Function.FunctionFormatException("Function should be sum, avg, min, max. Got " +
                        func.name());
        }
    }
}