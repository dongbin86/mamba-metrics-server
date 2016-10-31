package mamba.function;

import java.util.List;

/**
 * Created by dongbin on 2016/10/10.
 */
public class TimelineMetricsSeriesAvgAggregateFunction extends AbstractTimelineMetricsSeriesAggregateFunction {
    private static final String FUNCTION_NAME = "AVG";

    @Override
    protected Double applyFunction(List<Double> values) {
        double sum = 0.0d;
        for (Double value : values) {
            sum += value;
        }

        return sum / values.size();
    }

    @Override
    protected String getFunctionName() {
        return FUNCTION_NAME;
    }
}