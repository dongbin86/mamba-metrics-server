package mamba.function;


import mamba.aggregators.Function;

public enum SeriesAggregateFunction {
    AVG, MIN, MAX, SUM;

    public static boolean isPresent(String functionName) {
        try {
            SeriesAggregateFunction.valueOf(functionName.toUpperCase());
        } catch (IllegalArgumentException e) {
            return false;
        }
        return true;
    }

    public static SeriesAggregateFunction getFunction(String functionName) throws Function.FunctionFormatException {
        try {
            return SeriesAggregateFunction.valueOf(functionName.toUpperCase());
        } catch (NullPointerException | IllegalArgumentException e) {
            throw new Function.FunctionFormatException(
                    "Function should be sum, avg, min, max. Got " + functionName, e);
        }
    }
}
