package mamba.conf;

import mamba.store.TimelineMetricConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * @author dongbin  @Date: 10/28/16
 */


@Configuration
public class MetricConfiguration {

    @Bean
    public TimelineMetricConfiguration metricsConfiguration() {

        return new TimelineMetricConfiguration();
    }

}

