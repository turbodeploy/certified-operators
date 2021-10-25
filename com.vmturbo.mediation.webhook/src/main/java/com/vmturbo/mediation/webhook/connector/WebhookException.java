package com.vmturbo.mediation.webhook.connector;

import com.vmturbo.mediation.webhook.http.BasicHttpException;

/**
 * The wrapper exception class for all Webhook operation errors.
 */
public class WebhookException extends BasicHttpException {

    private static final long serialVersionUID = -7270838047059539519L;

    /**
     * Constructor when a http status code resulted in the exception.
     *
     * @param msg The message for the exception.
     * @param responseCode the status code for http.
     * @param responseBody the response body for http call.
     * @param cause the cause for the exception.
     */
    public WebhookException(String msg, Integer responseCode, String responseBody, final Throwable cause) {
        super(msg, responseCode, responseBody, cause);
    }

    /**
     * Class constructor.
     *
     * @param msg The input exception message.
     */
    public WebhookException(String msg) {
        super(msg, null, null, null);
    }
}
