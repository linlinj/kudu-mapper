package com.ruijie.giant.db.kudu.exception;

import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.Status;

/**
 * Created by jianglinlin on 2018/1/19.
 */
public class KuduOperationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public KuduOperationException() {
        super();
    }

    public KuduOperationException(String message) {
        super(message);
    }

    public KuduOperationException(Throwable cause) {
        super(cause);
    }

    public KuduOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
