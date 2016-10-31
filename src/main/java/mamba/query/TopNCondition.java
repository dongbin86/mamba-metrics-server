package mamba.query;

import mamba.aggregators.Function;
import mamba.metrics.Precision;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * Created by dongbin on 2016/10/10.
 */
public class TopNCondition extends DefaultCondition {

    private static final Log LOG = LogFactory.getLog(TopNCondition.class);
    private Integer topN;
    private boolean isBottomN;
    private Function topNFunction;

    public TopNCondition(List<String> metricNames, List<String> hostnames, String appId,
                         String instanceId, Long startTime, Long endTime, Precision precision,
                         Integer limit, boolean grouped, Integer topN, Function topNFunction,
                         boolean isBottomN) {
        super(metricNames, hostnames, appId, instanceId, startTime, endTime, precision, limit, grouped);
        this.topN = topN;
        this.isBottomN = isBottomN;
        this.topNFunction = topNFunction;
    }

    /**
     * Check if this is a case of Top N hosts condition
     *
     * @param metricNames A list of Strings.
     * @param hostnames   A list of Strings.
     * @return True if it is a Case of Top N Hosts (1 Metric and H hosts).
     */
    public static boolean isTopNHostCondition(List<String> metricNames, List<String> hostnames) {
        // Case 1 : 1 Metric, H hosts
        // Select Top N or Bottom N host series based on 1 metric (max/avg/sum)
        // Hostnames cannot be empty
        // Only 1 metric allowed, without wildcards
        return (CollectionUtils.isNotEmpty(hostnames) && metricNames.size() == 1 && !metricNamesHaveWildcard(metricNames));

    }

    /**
     * Check if this is a case of Top N metrics condition
     *
     * @param metricNames A list of Strings.
     * @param hostnames   A list of Strings.
     * @return True if it is a Case of Top N Metrics (M Metric and 1 or 0 host).
     */
    public static boolean isTopNMetricCondition(List<String> metricNames, List<String> hostnames) {
        // Case 2 : M Metric names or Regex, 1 or No host
        // Select Top N or Bottom N metric series based on metric values(max/avg/sum)
        // MetricNames cannot be empty
        // No host (aggregate) or 1 host allowed, without wildcards
        return (CollectionUtils.isNotEmpty(metricNames) && (hostnames == null || hostnames.size() <= 1) &&
                !hostNamesHaveWildcard(hostnames));
    }

    @Override
    public StringBuilder getConditionClause() {
        StringBuilder sb = new StringBuilder();
        boolean appendConjunction = false;

        if (isTopNHostCondition(metricNames, hostnames)) {
            appendConjunction = appendMetricNameClause(sb);

            StringBuilder hostnamesCondition = new StringBuilder();
            hostnamesCondition.append(" HOSTNAME IN (");
            hostnamesCondition.append(getTopNInnerQuery());
            hostnamesCondition.append(")");
            appendConjunction = append(sb, appendConjunction, getHostnames(), hostnamesCondition.toString());

        } else if (isTopNMetricCondition(metricNames, hostnames)) {

            StringBuilder metricNamesCondition = new StringBuilder();
            metricNamesCondition.append(" METRIC_NAME IN (");
            metricNamesCondition.append(getTopNInnerQuery());
            metricNamesCondition.append(")");
            appendConjunction = append(sb, appendConjunction, getMetricNames(), metricNamesCondition.toString());
            appendConjunction = appendHostnameClause(sb, appendConjunction);
        } else {
            LOG.error("Unsupported TopN Operation requested. Query can have either multiple hosts or multiple metric names " +
                    "but not both.");
            return null;
        }

        appendConjunction = append(sb, appendConjunction, getAppId(), " APP_ID = ?");
        appendConjunction = append(sb, appendConjunction, getInstanceId(), " INSTANCE_ID = ?");
        appendConjunction = append(sb, appendConjunction, getStartTime(), " SERVER_TIME >= ?");
        append(sb, appendConjunction, getEndTime(), " SERVER_TIME < ?");

        return sb;
    }

    public String getTopNInnerQuery() {
        String innerQuery = null;

        if (isTopNHostCondition(metricNames, hostnames)) {
            String groupByClause = "METRIC_NAME, HOSTNAME, APP_ID";
            String orderByClause = getTopNOrderByClause();

            innerQuery = String.format(PhoenixTransactSQL.TOP_N_INNER_SQL, PhoenixTransactSQL.getNaiveTimeRangeHint(getStartTime(), PhoenixTransactSQL.NATIVE_TIME_RANGE_DELTA),
                    "HOSTNAME", PhoenixTransactSQL.getTargetTableUsingPrecision(precision, true), super.getConditionClause().toString(),
                    groupByClause, orderByClause, topN);


        } else if (isTopNMetricCondition(metricNames, hostnames)) {

            String groupByClause = "METRIC_NAME, APP_ID";
            String orderByClause = getTopNOrderByClause();

            innerQuery = String.format(PhoenixTransactSQL.TOP_N_INNER_SQL, PhoenixTransactSQL.getNaiveTimeRangeHint(getStartTime(), PhoenixTransactSQL.NATIVE_TIME_RANGE_DELTA),
                    "METRIC_NAME", PhoenixTransactSQL.getTargetTableUsingPrecision(precision, (hostnames != null && hostnames.size() == 1)),
                    super.getConditionClause().toString(),
                    groupByClause, orderByClause, topN);
        }

        return innerQuery;
    }

    private String getTopNOrderByClause() {

        String orderByClause = null;

        if (topNFunction != null) {
            switch (topNFunction.getReadFunction()) {
                case AVG:
                    orderByClause = "ROUND(AVG(METRIC_SUM),2)";
                    break;
                case SUM:
                    orderByClause = "SUM(METRIC_SUM)";
                    break;
                default:
                    orderByClause = "MAX(METRIC_MAX)";
                    break;
            }
        }

        if (orderByClause == null) {
            orderByClause = "MAX(METRIC_MAX)";
        }

        if (!isBottomN) {
            orderByClause += " DESC";
        }

        return orderByClause;
    }

    public boolean isTopNHostCondition() {
        return isTopNHostCondition(metricNames, hostnames);
    }

    public boolean isTopNMetricCondition() {
        return isTopNMetricCondition(metricNames, hostnames);
    }

    public Integer getTopN() {
        return topN;
    }

    public void setTopN(Integer topN) {
        this.topN = topN;
    }

    public boolean isBottomN() {
        return isBottomN;
    }

    public void setIsBottomN(boolean isBottomN) {
        this.isBottomN = isBottomN;
    }

    public Function getTopNFunction() {
        return topNFunction;
    }

    public void setTopNFunction(Function topNFunction) {
        this.topNFunction = topNFunction;
    }
}
