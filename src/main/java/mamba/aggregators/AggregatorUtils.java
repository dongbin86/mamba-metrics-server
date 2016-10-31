package mamba.aggregators;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by dongbin on 2016/10/10.
 */
public class AggregatorUtils {

    private static final Log LOG = LogFactory.getLog(AggregatorUtils.class);
    public static Set<String> whitelistedMetrics = new HashSet<String>();

    public static double[] calculateAggregates(Map<Long, Double> metricValues) {
        double[] values = new double[4];
        double max = Double.MIN_VALUE;
        double min = Double.MAX_VALUE;
        double sum = 0.0;
        int metricCount = 0;

        if (metricValues != null && !metricValues.isEmpty()) {
            for (Double value : metricValues.values()) {
                // TODO: Some nulls in data - need to investigate null values from host
                if (value != null) {
                    if (value > max) {
                        max = value;
                    }
                    if (value < min) {
                        min = value;
                    }
                    sum += value;
                }
            }
            metricCount = metricValues.values().size();
        }
        // BR: WHY ZERO is a good idea?
        values[0] = sum;
        values[1] = max != Double.MIN_VALUE ? max : 0.0;
        values[2] = min != Double.MAX_VALUE ? min : 0.0;
        values[3] = metricCount;

        return values;
    }

    public static void populateMetricWhitelistFromFile(String whitelistFile) {

        FileInputStream fstream = null;
        BufferedReader br = null;
        String strLine;

        try {
            fstream = new FileInputStream(whitelistFile);
            br = new BufferedReader(new InputStreamReader(fstream));

            while ((strLine = br.readLine()) != null) {
                strLine = strLine.trim();
                whitelistedMetrics.add(strLine);
            }
        } catch (IOException ioEx) {
            LOG.error("Unable to parse metric whitelist file", ioEx);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                }
            }

            if (fstream != null) {
                try {
                    fstream.close();
                } catch (IOException e) {
                }
            }
        }
        LOG.info("Whitelisting " + whitelistedMetrics.size() + " metrics");
        LOG.debug("Whitelisted metrics : " + Arrays.toString(whitelistedMetrics.toArray()));
    }
}
