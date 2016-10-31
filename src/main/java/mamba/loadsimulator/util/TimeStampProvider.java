package mamba.loadsimulator.util;

/**
 * Created by sanbing on 10/10/16.
 */
public class TimeStampProvider {
    private int timeStep;
    private long currentTime;
    private int sendInterval;

    public TimeStampProvider(long startTime, int timeStep, int sendInterval) {
        this.timeStep = timeStep;
        this.currentTime = startTime - timeStep;
        this.sendInterval = sendInterval;
    }

    public long next() {
        return currentTime += timeStep;
    }

    public long[] timestampsForNextInterval() {
        return timestampsForInterval(sendInterval);
    }

    private long[] timestampsForInterval(int sendInterval) {
        int steps = sendInterval / timeStep;
        long[] timestamps = new long[steps];

        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = next();
        }

        return timestamps;
    }
}
