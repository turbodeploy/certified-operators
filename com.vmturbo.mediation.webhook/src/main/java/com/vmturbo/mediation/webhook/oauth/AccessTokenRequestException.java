package com.vmturbo.mediation.webhook.oauth;

import com.vmturbo.mediation.webhook.http.BasicHttpException;

/**
 * Exception class related to AccessTokenRequest failures.
 */
public class AccessTokenRequestException extends BasicHttpException {

    /**
     * Constructor when a http status code resulted in the exception.
     *
     * @param msg The message for the exception.
     * @param responseCode the status code for http.
     * @param responseBody the response body for http call.
     * @param cause the cause for the exception.
     */
    public AccessTokenRequestException(String msg, Integer responseCode, String responseBody, Throwable cause) {
        super(msg, responseCode, responseBody, cause);
    }
}
