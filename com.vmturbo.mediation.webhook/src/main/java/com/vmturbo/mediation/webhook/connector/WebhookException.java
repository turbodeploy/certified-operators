package com.vmturbo.mediation.webhook.connector;

import com.vmturbo.mediation.connector.common.HttpConnectorException;

/**
 * The wrapper exception class for all Webhook operation errors.
 */
public class WebhookException extends HttpConnectorException {

    private static final long serialVersionUID = -7270838047059539519L;

    private final Integer responseCode;
    private final String responseBody;

    /**
     * Constructor when a http status code resulted in the exception.
     *
     * @param msg The message for the exception.
     * @param responseCode the status code for http.
     * @param responseBody the response body for http call.
     * @param cause the cause for the exception.
     */
    public WebhookException(String msg, Integer responseCode, String responseBody, final Throwable cause) {
        super(msg, cause);
        this.responseCode = responseCode;
        this.responseBody = responseBody;
    }

    /**
     * Class constructor.
     *
     * @param msg The input exception message.
     */
    public WebhookException(String msg) {
        super(msg);
        this.responseCode = null;
        this.responseBody = null;
    }

    public Integer getResponseCode() {
        return responseCode;
    }

    public String getResponseBody() {
        return responseBody;
    }
}
