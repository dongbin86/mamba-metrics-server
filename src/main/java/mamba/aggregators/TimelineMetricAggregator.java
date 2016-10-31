package mamba.aggregators;

/**
 * Created by dongbin on 2016/10/10.
 */
public interface TimelineMetricAggregator extends Runnable {

    public boolean doWork(long startTime, long endTime);


    public boolean isDisabled();


    public Long getSleepIntervalMillis();
}
