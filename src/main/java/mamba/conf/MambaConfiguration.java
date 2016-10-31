package mamba.conf;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author dongbin  @Date: 10/29/16
 */


@Configuration
public class MambaConfiguration {


    @Bean
    public YarnConfiguration yarnConfiguration() {
        return new YarnConfiguration();
    }

}
