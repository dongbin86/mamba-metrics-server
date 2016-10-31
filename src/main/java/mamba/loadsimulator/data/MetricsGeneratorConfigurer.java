package mamba.loadsimulator.data;

import mamba.loadsimulator.util.RandomMetricsProvider;
import mamba.loadsimulator.util.TimeStampProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sanbing on 10/10/16.
 */
public class MetricsGeneratorConfigurer {

    private final static Logger LOG = LoggerFactory.getLogger
            (MetricsGeneratorConfigurer.class);

    /**
     * Creates HostMetricsGenerator configured with metric names loaded from file.
     *
     * @param id         ApplicationInstance descriptor, will be used to create
     *                   HostMetricsGenerator, cannot be null
     * @param timeStamps configured TimeStampProvider that can provide next
     *                   timestamp, cannot be null
     * @return HostMetricsGenerator with given ApplicationInstance id and configured
     * mapping of
     * metric names to data providers
     */
    public static HostMetricsGenerator createMetricsForHost(
            ApplicationInstance id,
            TimeStampProvider timeStamps) {
        return new HostMetricsGenerator(id, timeStamps, readMetrics(id.getAppId()));
    }

    private static Map<String, RandomMetricsProvider> readMetrics(AppID type) {
        InputStream input = null;
        Map<String, RandomMetricsProvider> metrics =
                new HashMap<String, RandomMetricsProvider>();
        String fileName = "metrics_def/" + type.toString() + ".dat";

        try {
            LOG.info("Loading " + fileName);

            input = MetricsGeneratorConfigurer.class.getClassLoader()
                    .getResourceAsStream(fileName);

            BufferedReader reader = new BufferedReader(new InputStreamReader(input));

            String line;
            while ((line = reader.readLine()) != null) {
                metrics.put(line.trim(), new RandomMetricsProvider(100, 200));
            }

        } catch (IOException e) {
            LOG.error("Cannot read file " + fileName + " for appID " + type.toString(), e);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException ex) {
                    // intentionally left blank, here we cannot do anything
                }
            }
        }

        return metrics;
    }
}
