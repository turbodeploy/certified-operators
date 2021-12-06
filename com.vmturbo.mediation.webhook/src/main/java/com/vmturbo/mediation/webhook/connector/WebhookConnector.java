package com.vmturbo.mediation.webhook.connector;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.function.BiFunction;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;

import com.vmturbo.mediation.connector.common.HttpConnector;
import com.vmturbo.mediation.connector.common.HttpConnectorException;
import com.vmturbo.mediation.connector.common.HttpConnectorFactory;
import com.vmturbo.mediation.connector.common.HttpConnectorFactoryBuilder;
import com.vmturbo.mediation.connector.common.HttpConnectorSettings;
import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.mediation.connector.common.Query;
import com.vmturbo.mediation.connector.common.Response;
import com.vmturbo.mediation.connector.common.http.query.converter.HttpQueryConverter;
import com.vmturbo.mediation.webhook.WebhookProperties;
import com.vmturbo.mediation.webhook.connector.WebHookQueries.WebhookQuery;
import com.vmturbo.mediation.webhook.http.BasicHttpSuccessResponseProcessor;
import com.vmturbo.mediation.webhook.http.ConnectorCommon;
import com.vmturbo.platform.sdk.common.util.WebhookConstants;

/**
 * Webhook connector.
 */
public class WebhookConnector implements HttpConnector, Closeable {

    private final HttpConnectorFactory<HttpConnectorSettings, WebhookCredentials> connectorFactory;
    private final WebhookCredentials credentials;
    private final WebhookQueryConverter webhookQueryConverter;

    /**
     * Constructor for webhook connector.
     *
     * @param webhookCredentials webhook credentials containing details of the connection parameters
     * @param propertyProvider property provide
     */
    public WebhookConnector(@Nonnull WebhookCredentials webhookCredentials,
            @Nonnull WebhookProperties propertyProvider) {
        this.credentials = Objects.requireNonNull(webhookCredentials);
        this.webhookQueryConverter = new WebhookQueryConverter(credentials);
        this.connectorFactory = getConnectorFactory(propertyProvider.getConnectionTimeout());
    }

    /**
     * Constructor directly injects webhook credentials and connector factory.
     *
     * @param webhookCredentials webhook credentials containing details of the connection parameters
     * @param connectorFactory connector factory
     */
    @VisibleForTesting
    public WebhookConnector(@Nonnull WebhookCredentials webhookCredentials,
            @Nonnull HttpConnectorFactory<HttpConnectorSettings, WebhookCredentials> connectorFactory) {
        this.credentials = webhookCredentials;
        this.connectorFactory = connectorFactory;
        this.webhookQueryConverter = new WebhookQueryConverter(credentials);
    }

    /**
     * Creates {@link HttpConnectorFactory} instance that used by the Webhook probe.
     *
     * @param timeout connection and socket timeout.
     * @return HTTP cached connector factory.
     */
    @Nonnull
    private HttpConnectorFactory<HttpConnectorSettings, WebhookCredentials> getConnectorFactory(
            final int timeout) {
        final HttpConnectorFactoryBuilder<HttpConnectorSettings, WebhookCredentials>
                connectorFactoryBuilder = createConnectorFactoryBuilder(timeout, credentials, webhookQueryConverter)
                // It doesn't hurt to set this, but it won't be used if we manually create the
                // HTTP client (below).
                .setTimeout(timeout);
        return connectorFactoryBuilder.build();
    }

    protected static HttpConnectorFactoryBuilder<HttpConnectorSettings, WebhookCredentials> createConnectorFactoryBuilder(
            final int timeout, WebhookCredentials webhookCredentials, WebhookQueryConverter webhookQueryConverter) {
        // TODO: register responseProcessors for succeeded and failed status codes specified for webhook;
        //  register queryConverters for other http method types
        return HttpConnectorFactory.<HttpConnectorSettings, WebhookCredentials>jsonConnectorFactoryBuilder()
                .setContextCreator(getContextCreator())
                .setHttpClient(ConnectorCommon.createHttpClient(timeout, webhookCredentials.isTrustSelfSignedCertificates()))
                .registerMethodTypeToQueryConverter(HttpMethodType.GET, webhookQueryConverter)
                .registerMethodTypeToQueryConverter(HttpMethodType.POST, webhookQueryConverter)
                .registerMethodTypeToQueryConverter(HttpMethodType.PUT, webhookQueryConverter)
                .registerMethodTypeToQueryConverter(HttpMethodType.DELETE, webhookQueryConverter)
                .registerMethodTypeToQueryConverter(HttpMethodType.PATCH, webhookQueryConverter)
                // registering all the code that has been listed as successful
                // in https://datatracker.ietf.org/doc/html/rfc2616#section-10.2
                .registerStatusCodeToResponseProcessor(HttpStatus.SC_OK,
                        new BasicHttpSuccessResponseProcessor())
                .registerStatusCodeToResponseProcessor(HttpStatus.SC_CREATED,
                        new BasicHttpSuccessResponseProcessor())
                .registerStatusCodeToResponseProcessor(HttpStatus.SC_ACCEPTED,
                        new BasicHttpSuccessResponseProcessor())
                .registerStatusCodeToResponseProcessor(HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION,
                        new BasicHttpSuccessResponseProcessor())
                .registerStatusCodeToResponseProcessor(HttpStatus.SC_NO_CONTENT,
                        new BasicHttpSuccessResponseProcessor())
                .registerStatusCodeToResponseProcessor(HttpStatus.SC_RESET_CONTENT,
                        new BasicHttpSuccessResponseProcessor())
                .registerStatusCodeToResponseProcessor(HttpStatus.SC_PARTIAL_CONTENT,
                        new BasicHttpSuccessResponseProcessor());
    }

    @Nonnull
    private static BiFunction<WebhookCredentials, HttpConnectorSettings, HttpClientContext> getContextCreator() {
        return (credentials, settings) -> {
            final HttpClientContext context = HttpClientContext.create();
            if (credentials.getAuthenticationMethod() == WebhookConstants.AuthenticationMethod.BASIC) {
                final CredentialsProvider credentialProvider = new BasicCredentialsProvider();

                // preemptively select basic authentication.
                final AuthCache authCache = new BasicAuthCache();
                context.setAuthCache(authCache);
                final String url = credentials.getUrlWithoutPath();
                authCache.put(HttpHost.create(url), new BasicScheme());

                credentialProvider.setCredentials(AuthScope.ANY,
                        new UsernamePasswordCredentials(credentials.getUserName(),
                                credentials.getPassword()));

                context.setCredentialsProvider(credentialProvider);
            }

            return context;
        };
    }

    /**
     * Close connection factory.
     *
     * @throws IOException thrown in case we are unable to close the connection
     */
    @Override
    public void close() throws IOException {
        if (this.connectorFactory != null) {
            this.connectorFactory.close();
        }
    }

    /**
     * Executes the provided webhook query against the endpoint defined in this connector.
     *
     * @param query the query to send.
     * @return the parsed response from the endpoint.
     * @throws InterruptedException if the blocking call was interrupted (ex: probe is shutting down).
     * @throws WebhookException if there was an issue contacting the webhook endpoint.
     */
    @Nonnull
    @Override
    public <T extends Response, V extends Query<T>> T execute(@Nonnull final V query)
            throws WebhookException, InterruptedException {
        try {
            return connectorFactory.getConnector(credentials).execute(query);
        } catch (HttpConnectorException originalException) {
            throw new WebhookException(
                "Failed to initialize webhook connection to URL " + credentials.getWebhookUrl(),
                originalException.getErrorStatus().orElse(null),
                originalException.getRawResponse(),
                originalException);
        }
    }

    /**
     * Gets the query converter used for converting queries in this connector.
     *
     * @return the query converter.
     */
    @Nonnull
    public WebhookQueryConverter getWebhookQueryConverter() {
        return webhookQueryConverter;
    }

    /**
     * HttpConnector does not provide a way to set the url directly. As a result, we override it
     * with our url here.
     */
    public static class WebhookQueryConverter
            implements HttpQueryConverter<HttpUriRequest, WebhookQuery, WebhookCredentials> {

        private final HttpRequestWithEntity httpRequestWithEntity;

        /**
         * WebhookQueryConverter constructor.
         *
         * @param webhookCredentials the data associated with the webhook request.
         */
        public WebhookQueryConverter(WebhookCredentials webhookCredentials) {
            httpRequestWithEntity = new HttpRequestWithEntity(webhookCredentials.getMethod(),
                    webhookCredentials.getWebhookUrl());
        }

        @Nonnull
        @Override
        public HttpUriRequest convert(@Nonnull final WebhookQuery webhookPostQuery,
                @Nonnull final WebhookCredentials webhookCredentials) throws IOException {
            // only set the body if it has been set
            if (webhookPostQuery.getBody().isPresent()) {
                httpRequestWithEntity.setEntity(
                        new StringEntity(webhookPostQuery.getBody().get().getWebhookBody()));
            }
            webhookPostQuery.getHeaders().forEach(httpRequestWithEntity::addHeader);

            return httpRequestWithEntity;
        }

        /**
         * Overwrites the first header with the same name. The new header will be appended to the
         * end of the list, if no header with the given name can be found
         *
         * @param name the header name.
         * @param value the header value.
         */
        public void setHeader(String name, String value) {
            httpRequestWithEntity.setHeader(name, value);
        }
    }

    /**
     * Encapsulates all data related to making a http request to the webhook endpoint.
     */
    public static final class HttpRequestWithEntity extends HttpEntityEnclosingRequestBase {

        private final String methodType;

        /**
         * HttpRequestWithEntity constructor.
         *
         * @param methodType the method type to be used for the http request.
         * @param url the url to make the http request to.
         */
        public HttpRequestWithEntity(String methodType, String url) {
            this.setURI(URI.create(url));
            this.methodType = methodType;
        }

        @Override
        public String getMethod() {
            return methodType;
        }
    }
}
