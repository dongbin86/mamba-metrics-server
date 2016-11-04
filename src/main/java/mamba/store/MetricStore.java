package mamba.store;

import mamba.metrics.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by dongbin on 2016/10/10.
 */
public interface MetricStore {

    TimelineMetrics getTimelineMetrics(List<String> metricNames, List<String> hostnames,
                                       String applicationId, String instanceId, Long startTime,
                                       Long endTime, Precision precision, Integer limit, boolean groupedByHosts,
                                       TopNConfig topNConfig, String seriesAggregateFunction)
            throws SQLException, IOException;


    TimelinePutResponse putMetrics(TimelineMetrics metrics) throws SQLException, IOException;


    TimelinePutResponse putContainerMetrics(List<ContainerMetric> metrics)
            throws SQLException, IOException;


    Map<String, List<TimelineMetricMetadata>> getTimelineMetricMetadata() throws SQLException, IOException;


    Map<String, Set<String>> getHostAppsMetadata() throws SQLException, IOException;
}
