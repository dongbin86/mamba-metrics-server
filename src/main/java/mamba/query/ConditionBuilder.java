package mamba.query;

import mamba.aggregators.Function;
import mamba.metrics.Precision;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by dongbin on 2016/10/10.
 */
public class ConditionBuilder {

    private List<String> metricNames;
    private List<String> hostnames;
    private String appId;
    private String instanceId;
    private Long startTime;
    private Long endTime;
    private Precision precision;
    private Integer limit;
    private boolean grouped;
    private boolean noLimit = false;
    private Integer fetchSize;
    private String statement;
    private Set<String> orderByColumns = new LinkedHashSet<String>();
    private Integer topN;
    private boolean isBottomN;
    private Function topNFunction;

    public ConditionBuilder(List<String> metricNames) {
        this.metricNames = metricNames;
    }

    public ConditionBuilder hostnames(List<String> hostnames) {
        this.hostnames = hostnames;
        return this;
    }

    public ConditionBuilder appId(String appId) {
        this.appId = appId;
        return this;
    }

    public ConditionBuilder instanceId(String instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    public ConditionBuilder startTime(Long startTime) {
        this.startTime = startTime;
        return this;
    }

    public ConditionBuilder endTime(Long endTime) {
        this.endTime = endTime;
        return this;
    }

    public ConditionBuilder precision(Precision precision) {
        this.precision = precision;
        return this;
    }

    public ConditionBuilder limit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public ConditionBuilder grouped(boolean grouped) {
        this.grouped = grouped;
        return this;
    }

    public ConditionBuilder noLimit(boolean noLimit) {
        this.noLimit = noLimit;
        return this;
    }

    public ConditionBuilder fetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public ConditionBuilder statement(String statement) {
        this.statement = statement;
        return this;
    }

    public ConditionBuilder orderByColumns(Set<String> orderByColumns) {
        this.orderByColumns = orderByColumns;
        return this;
    }

    public ConditionBuilder topN(Integer topN) {
        this.topN = topN;
        return this;
    }

    public ConditionBuilder isBottomN(boolean isBottomN) {
        this.isBottomN = isBottomN;
        return this;
    }

    public ConditionBuilder topNFunction(Function topNFunction) {
        this.topNFunction = topNFunction;
        return this;
    }

    public Condition build() {
        if (topN == null) {
            return new DefaultCondition(
                    metricNames,
                    hostnames, appId, instanceId, startTime, endTime,
                    precision, limit, grouped);
        } else {
            return new TopNCondition(metricNames, hostnames, appId, instanceId,
                    startTime, endTime, precision, limit, grouped, topN, topNFunction, isBottomN);
        }
    }

}
