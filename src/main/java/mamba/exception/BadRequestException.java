package mamba.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * @author dongbin  @Date: 10/29/16
 */
public class BadRequestException extends WebApplicationException {

    private static final long serialVersionUID = 1L;

    public BadRequestException() {
        super(Response.Status.BAD_REQUEST);
    }

    public BadRequestException(Throwable cause) {
        super(cause, Response.Status.BAD_REQUEST);
    }

    public BadRequestException(String msg) {
        super(new Exception(msg), Response.Status.BAD_REQUEST);
    }

}
