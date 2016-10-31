package mamba.loadsimulator.util;

import java.util.Random;

/**
 * Created by sanbing on 10/10/16.
 */
public class RandomMetricsProvider {

    private double min;
    private double max;
    private Random rnd;

    public RandomMetricsProvider(double min, double max) {
        this.min = min;
        this.max = max;
        this.rnd = new Random();
    }

    public double next() {
        return rnd.nextDouble() * (max - min) + min;
    }
}
