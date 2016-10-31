package mamba.entity;


import java.util.ArrayList;
import java.util.List;

/**
 * @author dongbin  @Date: 10/28/16
 */
public class TimelinePutResponse {

    private List<TimelinePutError> errors = new ArrayList<TimelinePutError>();

    public TimelinePutResponse() {

    }


    public List<TimelinePutError> getErrors() {
        return errors;
    }


    public void addError(TimelinePutError error) {
        errors.add(error);
    }


    public void addErrors(List<TimelinePutError> errors) {
        this.errors.addAll(errors);
    }


    public void setErrors(List<TimelinePutError> errors) {
        this.errors.clear();
        this.errors.addAll(errors);
    }


    public static class TimelinePutError {


        public static final int NO_START_TIME = 1;

        public static final int IO_EXCEPTION = 2;


        public static final int SYSTEM_FILTER_CONFLICT = 3;


        public static final int ACCESS_DENIED = 4;


        public static final int NO_DOMAIN = 5;


        public static final int FORBIDDEN_RELATION = 6;

        private String entityId;
        private String entityType;
        private int errorCode;


        public String getEntityId() {
            return entityId;
        }


        public void setEntityId(String entityId) {
            this.entityId = entityId;
        }


        public String getEntityType() {
            return entityType;
        }


        public void setEntityType(String entityType) {
            this.entityType = entityType;
        }


        public int getErrorCode() {
            return errorCode;
        }


        public void setErrorCode(int errorCode) {
            this.errorCode = errorCode;
        }

    }
}
