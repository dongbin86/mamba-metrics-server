package mamba.conf;

import mamba.store.HBaseMetricStore;
import mamba.store.TimelineMetricConfiguration;
import mamba.store.MetricStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * @author dongbin  @Date: 10/29/16
 */

@Configuration
@Import({MetricConfiguration.class})
public class TimelineMetricStoreConfiguration {


    @Autowired
    private TimelineMetricConfiguration metricConfiguration;

    @Bean
    public MetricStore timelineMetricStore() {
        return new HBaseMetricStore(metricConfiguration);
    }

}
