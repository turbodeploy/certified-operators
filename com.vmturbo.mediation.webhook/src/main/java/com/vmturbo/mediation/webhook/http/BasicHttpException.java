package com.vmturbo.mediation.webhook.http;

import javax.annotation.Nullable;

import com.vmturbo.mediation.connector.common.HttpConnectorException;

/**
 * The http exception that contains the response code and message body.
 */
public class BasicHttpException extends HttpConnectorException {

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
    public BasicHttpException(@Nullable String msg, @Nullable Integer responseCode,
                              @Nullable String responseBody, @Nullable final Throwable cause) {
        super(msg, cause);
        this.responseCode = responseCode;
        this.responseBody = responseBody;
    }

    @Nullable
    public Integer getResponseCode() {
        return responseCode;
    }

    @Nullable
    public String getResponseBody() {
        return responseBody;
    }

    @Override
    public String toString() {
        return getClass().getName() + "{ responseCode=" + responseCode
                + ", responseBody='" + responseBody + "'}";
    }
}
