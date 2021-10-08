package com.vmturbo.mediation.webhook.connector;

import java.util.Collection;
import java.util.Collections;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.http.Header;

import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.mediation.connector.common.http.AbstractHttpBodyAwareQuery;
import com.vmturbo.mediation.webhook.http.BasicHttpResponse;

/**
 * All objects related to Webhook queries and responses.
 */
public class WebHookQueries {

    private WebHookQueries() {
    }

    /**
     * Query converted by webhook connector into a http request.
     */
    public static class WebhookQuery extends AbstractHttpBodyAwareQuery<BasicHttpResponse, WebhookBody> {

        /**
         * Creates the query to send to the webhook endpoint.
         *
         * @param httpMethodType type of method of the request, decided by the target credentials.
         * @param body the body of the request to send to the webhook endpoint.
         * @param headers the headers of the http request.
         */
        public WebhookQuery(
                @Nonnull HttpMethodType httpMethodType,
                @Nullable WebhookBody body,
                @Nonnull Collection<Header> headers) {
            super(httpMethodType,
                    "", // not used because we overrode the query converter
                    Collections.emptyMap(), // not used because we overrode the query converter
                    body, // not used because we overrode the query converter
                    headers,
                    BasicHttpResponse.class);
        }
    }
}
