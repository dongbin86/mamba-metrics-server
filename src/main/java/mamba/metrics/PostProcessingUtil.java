package mamba.metrics;

import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialFunction;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by sanbing on 10/10/16.
 */
public class PostProcessingUtil {

    public static Map<Long, Double> interpolateMissingData(Map<Long, Double> metricValues, long expectedInterval) {

        if (metricValues == null)
            return null;

        Long prevTime = null;
        Double prevVal = null;
        Map<Long, Double> interpolatedMetricValues = new TreeMap<Long, Double>();

        for (Map.Entry<Long, Double> timeValueEntry : metricValues.entrySet()) {
            Long currTime = timeValueEntry.getKey();
            Double currVal = timeValueEntry.getValue();

            if (prevTime != null) {
                Long stepTime = prevTime;
                while ((currTime - stepTime) > expectedInterval) {
                    stepTime += expectedInterval;
                    double interpolatedValue = interpolate(stepTime,
                            prevTime, prevVal,
                            currTime, currVal);
                    interpolatedMetricValues.put(stepTime, interpolatedValue);
                }
            }

            interpolatedMetricValues.put(currTime, currVal);
            prevTime = currTime;
            prevVal = currVal;
        }
        return interpolatedMetricValues;
    }

    public static Double interpolate(Long t, Long t1, Double m1,
                                     Long t2, Double m2) {
        //Linear Interpolation : y = y0 + (y1 - y0) * ((x - x0) / (x1 - x0))
        if (m1 == null && m2 == null) {
            return null;
        }

        if (m1 == null)
            return m2;

        if (m2 == null)
            return m1;

        if (t1 == null || t2 == null)
            return null;

        double slope = (m2 - m1) / (t2 - t1);
        return m1 + slope * (t - t1);
    }

    public static Map<Long, Double> interpolate(Map<Long, Double> valuesMap, List<Long> requiredTimestamps) {

        LinearInterpolator linearInterpolator = new LinearInterpolator();

        if (valuesMap == null || valuesMap.isEmpty()) {
            return null;
        }
        if (requiredTimestamps == null || requiredTimestamps.isEmpty()) {
            return null;
        }

        Map<Long, Double> interpolatedValuesMap = new HashMap<>();

        if (valuesMap.size() == 1) {
            //Just one value present in the window. Use that value to interpolate all required timestamps.
            Double value = null;
            for (Map.Entry<Long, Double> entry : valuesMap.entrySet()) {
                value = entry.getValue();
            }
            for (Long requiredTs : requiredTimestamps) {
                interpolatedValuesMap.put(requiredTs, value);
            }
            return interpolatedValuesMap;
        }

        double[] timestamps = new double[valuesMap.size()];
        double[] metrics = new double[valuesMap.size()];

        int i = 0;
        for (Map.Entry<Long, Double> entry : valuesMap.entrySet()) {
            timestamps[i] = (double) entry.getKey();
            metrics[i++] = entry.getValue();
        }

        PolynomialSplineFunction function = linearInterpolator.interpolate(timestamps, metrics);
        PolynomialFunction[] splines = function.getPolynomials();
        PolynomialFunction first = splines[0];

        for (Long requiredTs : requiredTimestamps) {

            Double interpolatedValue = null;
            if (timestampInRange(requiredTs, timestamps[0], timestamps[timestamps.length - 1])) {
        /*
          Interpolation Case
          Required TS is within range of the set of values used for interpolation.
          Hence, we can use library to get the interpolated value.
         */
                interpolatedValue = function.value((double) requiredTs);
            } else {
        /*
        Extrapolation Case
        Required TS outside range of the set of values used for interpolation.
        We will use the coefficients to make best effort extrapolation
        y(x)= y1 + m * (x−x1)
        where, m = (y2−y1)/(x2−x1)
         */
                if (first.getCoefficients() != null && first.getCoefficients().length > 0) {
          /*
          y = c0 + c1x
          where c0, c1 are coefficients
          c1 will not be present if slope is zero.
           */
                    Double y1 = first.getCoefficients()[0];
                    Double m = (first.getCoefficients().length > 1) ? first.getCoefficients()[1] : 0.0;
                    interpolatedValue = y1 + m * (requiredTs - timestamps[0]);
                }
            }

            if (interpolatedValue != null && interpolatedValue >= 0.0) {
                interpolatedValuesMap.put(requiredTs, interpolatedValue);
            }
        }
        return interpolatedValuesMap;
    }

    private static boolean timestampInRange(Long timestamp, double left, double right) {
        return (timestamp >= left && timestamp <= right);
    }
}
