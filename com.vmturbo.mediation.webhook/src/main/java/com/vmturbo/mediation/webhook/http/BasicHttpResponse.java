package com.vmturbo.mediation.webhook.http;

import javax.annotation.concurrent.Immutable;

import com.vmturbo.mediation.connector.common.Response;

/**
 * Contains whether or not the webhook request succeeded.
 */
@Immutable
public class BasicHttpResponse implements Response {
    private final int responseCode;
    private final String responseBody;

    /**
     * Creates a web hook response object.
     *
     * @param responseCode the http status code.
     * @param responseBody the body of the response.
     */
    public BasicHttpResponse(int responseCode, String responseBody) {
        this.responseCode = responseCode;
        this.responseBody = responseBody;
    }

    /**
     * Return result of executing a webhook query.
     *
     * @return true if a webhook was executed successfully, otherwise false.
     */
    public String getResponseBody() {
        return responseBody;
    }

    /**
     * Returns the status code for the response code.
     *
     * @return the status code for the response code.
     */
    public int getResponseCode() {
        return responseCode;
    }
}
