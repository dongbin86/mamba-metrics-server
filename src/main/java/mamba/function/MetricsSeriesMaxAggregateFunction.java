package mamba.function;

import java.util.List;

/**
 * Created by dongbin on 2016/10/10.
 */
public class MetricsSeriesMaxAggregateFunction extends AbstractMetricsSeriesAggregateFunction {
    private static final String FUNCTION_NAME = "MAX";

    @Override
    protected Double applyFunction(List<Double> values) {
        double max = Double.MIN_VALUE;
        for (Double value : values) {
            if (value > max) {
                max = value;
            }
        }

        return max;
    }

    @Override
    protected String getFunctionName() {
        return FUNCTION_NAME;
    }
}