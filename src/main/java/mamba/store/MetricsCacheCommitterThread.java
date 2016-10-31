package mamba.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by dongbin on 2016/10/10.
 */
public class MetricsCacheCommitterThread implements Runnable {

    private static final Log LOG = LogFactory.getLog(MetricsCacheCommitterThread.class);
    private static PhoenixHBaseAccessor phoenixHBaseAccessor;

    public MetricsCacheCommitterThread(PhoenixHBaseAccessor phoenixHBaseAccessor) {
        this.phoenixHBaseAccessor = phoenixHBaseAccessor;
    }
    @Override
    public void run() {
        LOG.debug("Checking if metrics cache is empty");
        if (!phoenixHBaseAccessor.isInsertCacheEmpty()) {
            phoenixHBaseAccessor.commitMetricsFromCache();
        }/**
         insertCache只要不为空，那么这个committer线程就执行持久化操作，将数据插入到METRIC_RECORD 表
         */
    }
}
