package mamba.conf;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * @author dongbin  @Date: 10/28/16
 */


@Configuration
public class MetricConfiguration {

    @Bean
    public mamba.store.MetricConfiguration metricsConfiguration() {

        return new mamba.store.MetricConfiguration();
    }

}

