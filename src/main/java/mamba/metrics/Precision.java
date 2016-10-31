package mamba.metrics;

/**
 * Created by sanbing on 10/10/16.
 */
public enum Precision {

    SECONDS,
    MINUTES,
    HOURS,
    DAYS, Precision;

    public static class PrecisionFormatException extends IllegalArgumentException {
        public PrecisionFormatException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static mamba.metrics.Precision getPrecision(String precision) throws PrecisionFormatException {
        if (precision == null) {
            return null;
        }
        try {
            return Precision.valueOf(precision.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new PrecisionFormatException("precision should be seconds, " +
                    "minutes, hours or days", e);
        }
    }

    public static mamba.metrics.Precision getPrecision(long startTime, long endTime) {
        long HOUR = 3600000; // 1 hour
        long DAY = 86400000; // 1 day
        long timeRange = endTime - startTime;
        if (timeRange > 30 * DAY) {
            return Precision.DAYS;
        } else if (timeRange > 1 * DAY) {
            return Precision.HOURS;
        } else if (timeRange > 2 * HOUR) {
            return Precision.MINUTES;
        } else {
            return Precision.SECONDS;
        }
    }

    public static mamba.metrics.Precision getHigherPrecision(mamba.metrics.Precision precision) {

        if (precision == null)
            return null;

        if (precision.equals(Precision.SECONDS)) {
            return Precision.MINUTES;
        } else if (precision.equals(Precision.MINUTES)) {
            return Precision.HOURS;
        } else if (precision.equals(Precision.HOURS)) {
            return Precision.DAYS;
        } else {
            return null;
        }
    }

}
