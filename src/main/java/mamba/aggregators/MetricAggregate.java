package mamba.aggregators;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Created by dongbin on 2016/10/10.
 */
@JsonSubTypes({@JsonSubTypes.Type(value = MetricClusterAggregate.class), @JsonSubTypes.Type(value = MetricHostAggregate.class)})
public class MetricAggregate {

    private static final ObjectMapper mapper = new ObjectMapper();

    protected Double sum = 0.0;
    protected Double deviation;
    protected Double max = Double.MIN_VALUE;
    protected Double min = Double.MAX_VALUE;

    public MetricAggregate() {
    }

    MetricAggregate(Double sum, Double deviation, Double max,
                    Double min) {
        this.sum = sum;
        this.deviation = deviation;
        this.max = max;
        this.min = min;
    }

    public void updateSum(Double sum) {
        this.sum += sum;
    }

    public void updateMax(Double max) {
        if (max > this.max) {
            this.max = max;
        }
    }

    public void updateMin(Double min) {
        if (min < this.min) {
            this.min = min;
        }
    }

    @JsonProperty("sum")
    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    @JsonProperty("deviation")
    public Double getDeviation() {
        return deviation;
    }

    public void setDeviation(Double deviation) {
        this.deviation = deviation;
    }

    @JsonProperty("max")
    public Double getMax() {
        return max;
    }

    public void setMax(Double max) {
        this.max = max;
    }

    @JsonProperty("min")
    public Double getMin() {
        return min;
    }

    public void setMin(Double min) {
        this.min = min;
    }

    public String toJSON() throws IOException {
        return mapper.writeValueAsString(this);
    }
}
