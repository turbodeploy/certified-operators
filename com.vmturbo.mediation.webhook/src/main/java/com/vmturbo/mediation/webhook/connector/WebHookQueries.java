package com.vmturbo.mediation.webhook.connector;

import java.util.Collections;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.mediation.connector.common.Response;
import com.vmturbo.mediation.connector.common.http.AbstractHttpBodyAwareQuery;

/**
 * All objects related to Webhook queries and responses.
 */
public class WebHookQueries {

    private WebHookQueries() {
    }

    /**
     * Query converted by webhook connector into a http request.
     */
    public static class WebhookQuery extends AbstractHttpBodyAwareQuery<WebhookResponse, WebhookBody> {

        /**
         * Creates the query to send to the webhook endpoint.
         *
         * @param httpMethodType type of method of the request, decided by the target credentials.
         * @param body the body of the request to send to the webhook endpoint.
         */
        public WebhookQuery(
                @Nonnull HttpMethodType httpMethodType,
                @Nonnull WebhookBody body) {
            super(httpMethodType,
                    "", // not used because we overrode the query converter
                    Collections.emptyMap(), // not used because we overrode the query converter
                    body, // not used because we overrode the query converter
                    WebhookResponse.class);
        }
    }

    /**
     * Contains whether or not the webhook request succeeded.
     */
    @Immutable
    public static class WebhookResponse implements Response {

        private final String responseBody;

        /**
         * Creates a web hook response object.
         *
         * @param responseBody the body of the response.
         */
        public WebhookResponse(String responseBody) {
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
    }
}
