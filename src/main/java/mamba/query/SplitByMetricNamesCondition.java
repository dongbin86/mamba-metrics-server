package mamba.query;


import mamba.metrics.Precision;

import java.util.Collections;
import java.util.List;

/**
 * Created by dongbin on 2016/10/10.
 */
public class SplitByMetricNamesCondition implements Condition {
    private final Condition adaptee;
    private String currentMetric;

    public SplitByMetricNamesCondition(Condition condition) {
        this.adaptee = condition;
    }

    @Override
    public boolean isEmpty() {
        return adaptee.isEmpty();
    }

    @Override
    public List<String> getMetricNames() {
        return Collections.singletonList(currentMetric);
    }

    @Override
    public boolean isPointInTime() {
        return adaptee.isPointInTime();
    }

    @Override
    public boolean isGrouped() {
        return adaptee.isGrouped();
    }

    @Override
    public List<String> getHostnames() {
        return adaptee.getHostnames();
    }

    @Override
    public Precision getPrecision() {
        return adaptee.getPrecision();
    }

    @Override
    public void setPrecision(Precision precision) {
        adaptee.setPrecision(precision);
    }

    @Override
    public String getAppId() {
        return adaptee.getAppId();
    }

    @Override
    public String getInstanceId() {
        return adaptee.getInstanceId();
    }

    @Override
    public StringBuilder getConditionClause() {
        StringBuilder sb = new StringBuilder();
        boolean appendConjunction = false;

        if (getMetricNames() != null) {
            for (String name : getMetricNames()) {
                if (sb.length() > 1) {
                    sb.append(" OR ");
                }
                sb.append("METRIC_NAME = ?");
            }

            appendConjunction = true;
        }
        // TODO prevent user from using this method with multiple hostnames and SQL LIMIT clause
        if (getHostnames() != null && getHostnames().size() > 1) {
            StringBuilder hostnamesCondition = new StringBuilder();
            for (String hostname : getHostnames()) {
                if (hostnamesCondition.length() > 0) {
                    hostnamesCondition.append(" ,");
                } else {
                    hostnamesCondition.append(" HOSTNAME IN (");
                }
                hostnamesCondition.append('?');
            }
            hostnamesCondition.append(')');
            appendConjunction = DefaultCondition.append(sb, appendConjunction, getHostnames(), hostnamesCondition.toString());
        } else {
            appendConjunction = DefaultCondition.append(sb, appendConjunction, getHostnames(), " HOSTNAME = ?");
        }
        appendConjunction = DefaultCondition.append(sb, appendConjunction,
                getAppId(), " APP_ID = ?");
        appendConjunction = DefaultCondition.append(sb, appendConjunction,
                getInstanceId(), " INSTANCE_ID = ?");
        appendConjunction = DefaultCondition.append(sb, appendConjunction,
                getStartTime(), " SERVER_TIME >= ?");
        DefaultCondition.append(sb, appendConjunction, getEndTime(),
                " SERVER_TIME < ?");

        return sb;
    }

    @Override
    public String getOrderByClause(boolean asc) {
        return adaptee.getOrderByClause(asc);
    }

    @Override
    public String getStatement() {
        return adaptee.getStatement();
    }

    @Override
    public void setStatement(String statement) {
        adaptee.setStatement(statement);
    }

    @Override
    public Long getStartTime() {
        return adaptee.getStartTime();
    }

    @Override
    public Long getEndTime() {
        return adaptee.getEndTime();
    }

    @Override
    public Integer getLimit() {
        return adaptee.getLimit();
    }

    @Override
    public Integer getFetchSize() {
        return adaptee.getFetchSize();
    }

    @Override
    public void setFetchSize(Integer fetchSize) {
        adaptee.setFetchSize(fetchSize);
    }

    @Override
    public void addOrderByColumn(String column) {
        adaptee.addOrderByColumn(column);
    }

    @Override
    public void setNoLimit() {
        adaptee.setNoLimit();
    }

    @Override
    public boolean doUpdate() {
        return false;
    }

    public List<String> getOriginalMetricNames() {
        return adaptee.getMetricNames();
    }

    public void setCurrentMetric(String currentMetric) {
        this.currentMetric = currentMetric;
    }
}
