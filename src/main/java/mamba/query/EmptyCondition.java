package mamba.query;


import mamba.metrics.Precision;

import java.util.List;

/**
 * Created by dongbin on 2016/10/10.
 */
public class EmptyCondition implements Condition {
    String statement;
    boolean doUpdate = false;

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public List<String> getMetricNames() {
        return null;
    }

    @Override
    public boolean isPointInTime() {
        return false;
    }

    @Override
    public boolean isGrouped() {
        return true;
    }

    @Override
    public List<String> getHostnames() {
        return null;
    }

    @Override
    public Precision getPrecision() {
        return null;
    }

    @Override
    public void setPrecision(Precision precision) {

    }

    @Override
    public String getAppId() {
        return null;
    }

    @Override
    public String getInstanceId() {
        return null;
    }

    @Override
    public StringBuilder getConditionClause() {
        return null;
    }

    @Override
    public String getOrderByClause(boolean asc) {
        return null;
    }

    @Override
    public String getStatement() {
        return statement;
    }

    @Override
    public void setStatement(String statement) {
        this.statement = statement;
    }

    @Override
    public Long getStartTime() {
        return null;
    }

    @Override
    public Long getEndTime() {
        return null;
    }

    @Override
    public Integer getLimit() {
        return null;
    }

    @Override
    public Integer getFetchSize() {
        return null;
    }

    @Override
    public void setFetchSize(Integer fetchSize) {

    }

    @Override
    public void addOrderByColumn(String column) {

    }

    @Override
    public void setNoLimit() {

    }

    public void setDoUpdate(boolean doUpdate) {
        this.doUpdate = doUpdate;
    }

    @Override
    public boolean doUpdate() {
        return doUpdate;
    }

    @Override
    public String toString() {
        return "EmptyCondition{ " +
                " statement = " + this.getStatement() +
                " doUpdate = " + this.doUpdate() +
                " }";
    }
}
