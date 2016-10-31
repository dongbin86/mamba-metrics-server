package mamba.aggregators;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by dongbin on 2016/10/10.
 */
public class MetricClusterAggregate extends MetricAggregate {

    private int numberOfHosts;

    @JsonCreator
    public MetricClusterAggregate() {
    }

    public MetricClusterAggregate(Double sum, int numberOfHosts, Double deviation,
                                  Double max, Double min) {
        super(sum, deviation, max, min);
        this.numberOfHosts = numberOfHosts;
    }

    @JsonProperty("numberOfHosts")
    public int getNumberOfHosts() {
        return numberOfHosts;
    }

    public void setNumberOfHosts(int numberOfHosts) {
        this.numberOfHosts = numberOfHosts;
    }

    public void updateNumberOfHosts(int count) {
        this.numberOfHosts += count;
    }

    /**
     * Find and update min, max and avg for a minute
     */
    public void updateAggregates(MetricClusterAggregate hostAggregate) {
        updateMax(hostAggregate.getMax());
        updateMin(hostAggregate.getMin());
        updateSum(hostAggregate.getSum());
        updateNumberOfHosts(hostAggregate.getNumberOfHosts());
    }

    @Override
    public String toString() {
        return "MetricAggregate{" +
                "sum=" + sum +
                ", numberOfHosts=" + numberOfHosts +
                ", deviation=" + deviation +
                ", max=" + max +
                ", min=" + min +
                '}';
    }
}
