package mamba.conf;

import mamba.entity.LeveldbTimelineStore;
import mamba.entity.MemoryTimelineStore;
import mamba.entity.TimelineStore;
import mamba.store.TimelineMetricConfiguration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * @author dongbin  @Date: 10/29/16
 */


@Configuration
@Import({MambaConfiguration.class})
public class TimelineStoreConfiguration {


    @Autowired
    private YarnConfiguration configuration;

    @Bean
    public TimelineStore timelineStore() {
        if (configuration.getBoolean(TimelineMetricConfiguration.DISABLE_APPLICATION_TIMELINE_STORE, true)) {
            return new MemoryTimelineStore();
        }
        return ReflectionUtils.newInstance(configuration.getClass(
                YarnConfiguration.TIMELINE_SERVICE_STORE, LeveldbTimelineStore.class,
                TimelineStore.class), configuration);
    }

}
