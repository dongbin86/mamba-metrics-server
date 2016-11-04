package mamba.function;

import java.util.List;

/**
 * Created by dongbin on 2016/10/10.
 */
public class MetricsSeriesMinAggregateFunction extends AbstractMetricsSeriesAggregateFunction {
    private static final String FUNCTION_NAME = "MIN";

    @Override
    protected Double applyFunction(List<Double> values) {
        double min = Double.MAX_VALUE;
        for (Double value : values) {
            if (value < min) {
                min = value;
            }
        }

        return min;
    }

    @Override
    protected String getFunctionName() {
        return FUNCTION_NAME;
    }
}