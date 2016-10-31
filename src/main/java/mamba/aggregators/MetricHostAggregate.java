package mamba.aggregators;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by dongbin on 2016/10/10.
 */
public class MetricHostAggregate extends MetricAggregate {
    private long numberOfSamples = 0;

    @JsonCreator
    public MetricHostAggregate() {
        super(0.0, 0.0, Double.MIN_VALUE, Double.MAX_VALUE);
    }

    public MetricHostAggregate(Double sum, int numberOfSamples,
                               Double deviation,
                               Double max, Double min) {
        super(sum, deviation, max, min);
        this.numberOfSamples = numberOfSamples;
    }

    @JsonProperty("numberOfSamples")
    public long getNumberOfSamples() {
        return numberOfSamples == 0 ? 1 : numberOfSamples;
    }

    public void setNumberOfSamples(long numberOfSamples) {
        this.numberOfSamples = numberOfSamples;
    }

    public void updateNumberOfSamples(long count) {
        this.numberOfSamples += count;
    }

    public double getAvg() {
        return sum / numberOfSamples;
    }

    /**
     * Find and update min, max and avg for a minute
     */
    public void updateAggregates(MetricHostAggregate hostAggregate) {
        updateMax(hostAggregate.getMax());
        updateMin(hostAggregate.getMin());
        updateSum(hostAggregate.getSum());
        updateNumberOfSamples(hostAggregate.getNumberOfSamples());
    }

    @Override
    public String toString() {
        return "MetricHostAggregate{" +
                "sum=" + sum +
                ", numberOfSamples=" + numberOfSamples +
                ", deviation=" + deviation +
                ", max=" + max +
                ", min=" + min +
                '}';
    }
}
