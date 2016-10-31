package mamba.metrics;

/**
 * Created by sanbing on 10/10/16.
 */
public class PrecisionLimitExceededException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    public PrecisionLimitExceededException(String message, Throwable cause) {
        super(message, cause);
    }

    public PrecisionLimitExceededException(String message) {
        super(message);
    }

    public PrecisionLimitExceededException(Throwable cause) {
        super(cause);
    }

}
