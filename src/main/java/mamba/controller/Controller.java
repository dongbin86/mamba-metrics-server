package mamba.controller;

import com.google.common.base.Joiner;
import mamba.entity.*;
import mamba.exception.BadRequestException;
import mamba.metrics.*;
import mamba.store.MetricStore;
import mamba.entity.TimelineReader.Field;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * @author dongbin  @Date: 10/29/16
 */
@RestController
@RequestMapping("/ws/v1/timeline")
public class Controller {

    private static final Log LOG = LogFactory.getLog(Controller.class);

    public static final Joiner CSV_JOINER = Joiner.on(',');

    @Autowired
    private MetricStore metricStore;


    @RequestMapping(value = "/about", method = RequestMethod.GET)
    public AboutInfo about() {
        return new AboutInfo("Timeline API");
    }



    @RequestMapping(value = "/metrics", method = RequestMethod.POST)
    @ResponseBody
    public TimelinePutResponse postMetrics(@RequestBody TimelineMetrics metrics) {

        if (metrics == null) {
            return new TimelinePutResponse();
        }

        try {

            if (LOG.isDebugEnabled()) {
                LOG.debug("Storing metrics: " +
                        TimelineUtils.dumpTimelineRecordtoJSON(metrics, true));
            }

            return metricStore.putMetrics(metrics);

        } catch (Exception e) {
            LOG.error("Error saving metrics.", e);
            throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/containermetrics", method = RequestMethod.POST)
    @ResponseBody
    public TimelinePutResponse postContainerMetrics(@RequestBody List<ContainerMetric> metrics) {
        if (metrics == null || metrics.isEmpty()) {
            return new TimelinePutResponse();
        }

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Storing container metrics: " + TimelineUtils
                        .dumpTimelineRecordtoJSON(metrics, true));
            }

            return metricStore.putContainerMetrics(metrics);

        } catch (Exception e) {
            LOG.error("Error saving metrics.", e);
            throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }


    @RequestMapping(value = "/metrics", method = RequestMethod.GET)
    public TimelineMetrics getTimelineMetrics(
            @RequestParam(value = "metricNames", required = true) String metricNames,
            @RequestParam(value = "appId", required = false) String appId,
            @RequestParam(value = "instanceId", required = false) String instanceId,
            @RequestParam(value = "hostname", required = false) String hostname,
            @RequestParam(value = "startTime", required = false) String startTime,
            @RequestParam(value = "endTime", required = false) String endTime,
            @RequestParam(value = "precision", required = false) String precision,
            @RequestParam(value = "limit", required = false) String limit,
            @RequestParam(value = "grouped", required = false) String grouped,
            @RequestParam(value = "topN", required = false) String topN,
            @RequestParam(value = "topNFunction", required = false) String topNFunction,
            @RequestParam(value = "isBottomN", required = false) String isBottomN,
            @RequestParam(value = "seriesAggregateFunction", required = false) String seriesAggregateFunction

    ) {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Request for metrics => metricNames: " + metricNames + ", " +
                        "appId: " + appId + ", instanceId: " + instanceId + ", " +
                        "hostname: " + hostname + ", startTime: " + startTime + ", " +
                        "endTime: " + endTime + ", " +
                        "precision: " + precision + "seriesAggregateFunction: " + seriesAggregateFunction);
            }

            System.out.println("********************************************************");

            System.out.println(metricNames);
            System.out.println(appId);
            System.out.println(hostname);

            System.out.println("********************************************************");


            return metricStore.getTimelineMetrics(
                    parseListStr(metricNames, ","), parseListStr(hostname, ","), appId, instanceId,
                    parseLongStr(startTime), parseLongStr(endTime),
                    Precision.getPrecision(precision), parseIntStr(limit),
                    parseBoolean(grouped), parseTopNConfig(topN, topNFunction, isBottomN),
                    seriesAggregateFunction);

        } catch (NumberFormatException ne) {
            throw new BadRequestException("startTime and limit should be numeric " +
                    "values");
        } catch (Precision.PrecisionFormatException pfe) {
            throw new BadRequestException("precision should be seconds, minutes " +
                    "or hours");
        } catch (PrecisionLimitExceededException iae) {
            throw new PrecisionLimitExceededException(iae.getMessage());
        } catch (IllegalArgumentException iae) {
            throw new BadRequestException(iae.getMessage());
        } catch (SQLException | IOException e) {
            throw new WebApplicationException(e,
                    Response.Status.INTERNAL_SERVER_ERROR);
        }

    }

    @RequestMapping(value = "/metrics/metadata", method = RequestMethod.GET)
    public Map<String, List<TimelineMetricMetadata>> getTimelineMetricMetadata() {
        try {
            return metricStore.getTimelineMetricMetadata();
        } catch (Exception e) {
            throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }

    }

    @RequestMapping(value = "/metrics/hosts", method = RequestMethod.GET)
    public Map<String, Set<String>> getHostedAppsMetadata() {

        try {
            return metricStore.getHostAppsMetadata();
        } catch (Exception e) {
            throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }

    }


    private static SortedSet<String> parseArrayStr(String str, String delimiter) {
        if (str == null) {
            return null;
        }
        SortedSet<String> strSet = new TreeSet<String>();
        String[] strs = str.split(delimiter);
        for (String aStr : strs) {
            strSet.add(aStr.trim());
        }
        return strSet;
    }

    private static NameValuePair parsePairStr(String str, String delimiter) {
        if (str == null) {
            return null;
        }
        String[] strs = str.split(delimiter, 2);
        try {
            return new NameValuePair(strs[0].trim(),
                    GenericObjectMapper.OBJECT_READER.readValue(strs[1].trim()));
        } catch (Exception e) {
            // didn't work as an Object, keep it as a String
            return new NameValuePair(strs[0].trim(), strs[1].trim());
        }
    }

    private static Collection<NameValuePair> parsePairsStr(
            String str, String aDelimiter, String pDelimiter) {
        if (str == null) {
            return null;
        }
        String[] strs = str.split(aDelimiter);
        Set<NameValuePair> pairs = new HashSet<NameValuePair>();
        for (String aStr : strs) {
            pairs.add(parsePairStr(aStr, pDelimiter));
        }
        return pairs;
    }

    private static EnumSet<Field> parseFieldsStr(String str, String delimiter) {
        if (str == null) {
            return null;
        }
        String[] strs = str.split(delimiter);
        List<Field> fieldList = new ArrayList<Field>();
        for (String s : strs) {
            s = s.trim().toUpperCase();
            if (s.equals("EVENTS")) {
                fieldList.add(Field.EVENTS);
            } else if (s.equals("LASTEVENTONLY")) {
                fieldList.add(Field.LAST_EVENT_ONLY);
            } else if (s.equals("RELATEDENTITIES")) {
                fieldList.add(Field.RELATED_ENTITIES);
            } else if (s.equals("PRIMARYFILTERS")) {
                fieldList.add(Field.PRIMARY_FILTERS);
            } else if (s.equals("OTHERINFO")) {
                fieldList.add(Field.OTHER_INFO);
            } else {
                throw new IllegalArgumentException("Requested nonexistent field " + s);
            }
        }
        if (fieldList.size() == 0) {
            return null;
        }
        Field f1 = fieldList.remove(fieldList.size() - 1);
        if (fieldList.size() == 0) {
            return EnumSet.of(f1);
        } else {
            return EnumSet.of(f1, fieldList.toArray(new Field[fieldList.size()]));
        }
    }

    private static Long parseLongStr(String str) {
        return str == null ? null : Long.parseLong(str.trim());
    }

    private static Integer parseIntStr(String str) {
        return str == null ? null : Integer.parseInt(str.trim());
    }

    private static boolean parseBoolean(String booleanStr) {
        return booleanStr == null || Boolean.parseBoolean(booleanStr);
    }

    private static TopNConfig parseTopNConfig(String topN, String topNFunction,
                                              String bottomN) {
        if (topN == null || topN.isEmpty()) {
            return null;
        }
        Integer topNValue = parseIntStr(topN);

        if (topNValue == 0) {
            LOG.info("Invalid Input for TopN query. Ignoring TopN Request.");
            return null;
        }

        Boolean isBottomN = (bottomN != null && Boolean.parseBoolean(bottomN));
        return new TopNConfig(topNValue, topNFunction, isBottomN);
    }


    private static List<String> parseListStr(String str, String delimiter) {
        if (str == null || str.trim().isEmpty()) {
            return null;
        }

        String[] split = str.trim().split(delimiter);
        List<String> list = new ArrayList<String>(split.length);
        for (String s : split) {
            if (!s.trim().isEmpty()) {
                list.add(s);
            }
        }

        return list;
    }

    private static String parseStr(String str) {
        return str == null ? null : str.trim();
    }


    public static class AboutInfo {

        private String about;

        public AboutInfo() {

        }

        public AboutInfo(String about) {
            this.about = about;
        }


        public String getAbout() {
            return about;
        }

        public void setAbout(String about) {
            this.about = about;
        }

    }

}
