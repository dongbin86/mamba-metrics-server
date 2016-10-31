package mamba.query;


import mamba.metrics.Precision;

import java.util.List;

/**
 * Created by dongbin on 2016/10/10.
 */
public interface Condition {
    boolean isEmpty();

    List<String> getMetricNames();

    boolean isPointInTime();

    boolean isGrouped();

    List<String> getHostnames();

    Precision getPrecision();

    void setPrecision(Precision precision);

    String getAppId();

    String getInstanceId();

    StringBuilder getConditionClause();

    String getOrderByClause(boolean asc);

    String getStatement();

    void setStatement(String statement);

    Long getStartTime();

    Long getEndTime();

    Integer getLimit();

    Integer getFetchSize();

    void setFetchSize(Integer fetchSize);

    void addOrderByColumn(String column);

    void setNoLimit();

    boolean doUpdate();
}
