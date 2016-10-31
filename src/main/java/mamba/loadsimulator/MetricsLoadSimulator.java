package mamba.loadsimulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sanbing on 2016/10/10.
 */
public class MetricsLoadSimulator {

    private final static Logger LOG = LoggerFactory.getLogger(MetricsLoadSimulator
            .class);

    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, String> mapArgs = parseArgs(args);

        LoadRunner loadRunner = new LoadRunner(
                mapArgs.get("hostName"),
                Integer.valueOf(mapArgs.get("numberOfHosts")),
                mapArgs.get("metricsHostName"),
                Integer.valueOf(mapArgs.get("collectInterval")),
                Integer.valueOf(mapArgs.get("sendInterval")),
                Boolean.valueOf(mapArgs.get("master"))
        );

        loadRunner.start();
    }

    /*
     Another entry point to the test to be called from ./jmetertest/AMSJMeterLoadTest.java
     */
    public static void startTest(Map<String, String> mapArgs) {

        LoadRunner loadRunner = new LoadRunner(
                mapArgs.get("hostName"),
                Integer.valueOf(mapArgs.get("numberOfHosts")),
                mapArgs.get("metricsHostName"),
                Integer.valueOf(mapArgs.get("collectInterval")),
                Integer.valueOf(mapArgs.get("sendInterval")),
                Boolean.valueOf(mapArgs.get("master"))
        );

        loadRunner.start();
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> mapProps = new HashMap<String, String>();
        mapProps.put("hostName", "host");
        mapProps.put("numberOfHosts", "20");
        mapProps.put("trafficType", "burst");
        mapProps.put("metricsHostName", "localhost");
        mapProps.put("collectInterval", "10000");
        mapProps.put("sendInterval", "60000");
        mapProps.put("master", "true");
        /*if (args.length == 0) {
            printUsage();
            throw new RuntimeException("Unexpected argument, See usage message.");
        } else {
            for (int i = 0; i < args.length; i += 2) {
                String arg = args[i];
                if (arg.equals("-h")) {
                    mapProps.put("hostName", args[i + 1]);
                } else if (arg.equals("-n")) {
                    mapProps.put("numberOfHosts", args[i + 1]);
                } else if (arg.equals("-t")) {
                    mapProps.put("trafficType", args[i + 1]);
                } else if (arg.equals("-m")) {
                    mapProps.put("metricsHostName", args[i + 1]);
                } else if (arg.equals("-c")) {
                    mapProps.put("collectInterval", args[i + 1]);
                } else if (arg.equals("-s")) {
                    mapProps.put("sendInterval", args[i + 1]);
                } else if (arg.equals("-M")) {
                    mapProps.put("master", args[i + 1]);
                } else if (arg.equals("-d")) {
                    // a dummy switch - it says that we agree with defaults
                } else {
                    printUsage();
                    throw new RuntimeException("Unexpected argument, See usage message.");
                }
            }
        }
*/
        LOG.info("Recognized options: baseHostName={} hosts#={} trafficMode={} " +
                        "metricsHostName={} collectIntervalMillis={} sendIntervalMillis={} " +
                        "simulateMaster={}",
                mapProps.get("hostName"),
                Integer.valueOf(mapProps.get("numberOfHosts")),
                mapProps.get("trafficType"),
                mapProps.get("metricsHostName"),
                Integer.valueOf(mapProps.get("collectInterval")),
                Integer.valueOf(mapProps.get("sendInterval")),
                Boolean.valueOf(mapProps.get("master"))
        );

        return mapProps;
    }

    public static void printUsage() {
        System.err.println("Usage: java MetricsLoadSimulator [OPTIONS]");
        System.err.println("Options: ");
        System.err.println("[-h hostName] [-n numberOfHosts] "
                + "[-t trafficMode {burst, staggered}] [-m metricsHostName] "
                + "[-c collectIntervalMillis {10 sec}] [-s sendIntervalMillis {60 sec}]"
                + "[-M simulateMaster {true, false}] ");
        System.err.println();
        System.err.println("When you select a master, then one simulated host will play");
        System.err.println("a role of a master, and the rest will be slaves. Otherwise");
        System.err.println("all simulation threads (single thread is for single host)");
        System.err.println("will be slave hosts");
    }

}
