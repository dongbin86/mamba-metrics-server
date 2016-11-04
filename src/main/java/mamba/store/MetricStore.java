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

    Metrics getTimelineMetrics(List<String> metricNames, List<String> hostnames,
                                       String applicationId, String instanceId, Long startTime,
                                       Long endTime, Precision precision, Integer limit, boolean groupedByHosts,
                                       TopNConfig topNConfig, String seriesAggregateFunction)
            throws SQLException, IOException;


    PutResponse putMetrics(Metrics metrics) throws SQLException, IOException;


    PutResponse putContainerMetrics(List<ContainerMetric> metrics)
            throws SQLException, IOException;


    Map<String, List<MetricMetadata>> getTimelineMetricMetadata() throws SQLException, IOException;


    Map<String, Set<String>> getHostAppsMetadata() throws SQLException, IOException;
}
