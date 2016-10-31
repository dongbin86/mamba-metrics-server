package mamba.store;

/**
 * Created by dongbin on 2016/10/10.
 */
public class MetricsSystemInitializationException extends RuntimeException {
    public MetricsSystemInitializationException() {
    }

    public MetricsSystemInitializationException(String msg) {
        super(msg);
    }

    public MetricsSystemInitializationException(Throwable t) {
        super(t);
    }

    public MetricsSystemInitializationException(String msg, Throwable t) {
        super(msg, t);
    }

}
