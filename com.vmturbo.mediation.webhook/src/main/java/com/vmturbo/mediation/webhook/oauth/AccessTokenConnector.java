package com.vmturbo.mediation.webhook.oauth;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.Nonnull;

import org.apache.http.client.methods.HttpUriRequest;

import com.vmturbo.mediation.connector.common.DefaultParametersBasedBodyTransformer;
import com.vmturbo.mediation.connector.common.HttpConnector;
import com.vmturbo.mediation.connector.common.HttpConnectorException;
import com.vmturbo.mediation.connector.common.HttpConnectorFactory;
import com.vmturbo.mediation.connector.common.HttpConnectorSettings;
import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.mediation.connector.common.Query;
import com.vmturbo.mediation.connector.common.Response;
import com.vmturbo.mediation.connector.common.http.query.converter.HttpQueryConverter;
import com.vmturbo.mediation.webhook.connector.WebhookConnector.HttpRequestWithEntity;
import com.vmturbo.mediation.webhook.http.ConnectorCommon;

/**
 * Access token connector.
 */
public class AccessTokenConnector implements HttpConnector, Closeable {

    private final OAuthCredentials credentials;
    private final HttpConnectorFactory<HttpConnectorSettings, OAuthCredentials>
            httpConnectorFactory;

    /**
     * AccessTokenConnector constructor.
     *
     * @param credentials the credentials used to construct oauth request.
     * @param timeout the timeout used for the http request.
     */
    public AccessTokenConnector(@Nonnull OAuthCredentials credentials, int timeout) {
        this.credentials = credentials;
        httpConnectorFactory = createConnectorFactory(credentials, timeout);
    }

    /**
     * AccessTokenConnector constructor.
     *
     * @param credentials the credentials used to construct oauth request.
     * @param connectorFactory connector factory
     */
    public AccessTokenConnector(@Nonnull OAuthCredentials credentials,
            HttpConnectorFactory<HttpConnectorSettings, OAuthCredentials> connectorFactory) {
        this.credentials = credentials;
        this.httpConnectorFactory = connectorFactory;
    }

    protected static HttpConnectorFactory<HttpConnectorSettings, OAuthCredentials> createConnectorFactory(
            OAuthCredentials credentials, int timeout) {
        return HttpConnectorFactory.<HttpConnectorSettings, OAuthCredentials>jsonConnectorFactoryBuilder()
                .setHttpClient(ConnectorCommon.createHttpClient(timeout, credentials.isTrustSelfSignedCertificates()))
                .registerMethodTypeToQueryConverter(HttpMethodType.POST,
                        new HttpQueryConverter<HttpUriRequest, AccessTokenQuery, OAuthCredentials>() {
                            @Nonnull
                            @Override
                            public HttpUriRequest convert(@Nonnull AccessTokenQuery httpQuery,
                                    @Nonnull OAuthCredentials credentials) {
                                HttpRequestWithEntity httpRequestWithEntity =
                                        new HttpRequestWithEntity(HttpMethodType.POST.name(),
                                                credentials.getOAuthUrl());
                                httpQuery.getBody().ifPresent(body -> {
                                    DefaultParametersBasedBodyTransformer
                                            defaultParametersBasedBodyTransformer =
                                            new DefaultParametersBasedBodyTransformer();
                                    httpRequestWithEntity.setEntity(
                                            defaultParametersBasedBodyTransformer.transform(body));
                                });
                                return httpRequestWithEntity;
                            }
                        })
                .build();
    }

    /**
     * Executes the provided access token query against the endpoint defined in this connector.
     *
     * @param query the query to send.
     * @return the parsed response from the endpoint.
     * @throws InterruptedException if the blocking call was interrupted (ex: probe is
     *         shutting down).
     * @throws HttpConnectorException if there was an issue contacting the oauth endpoint.
     */
    @Nonnull
    @Override
    public <T extends Response, V extends Query<T>> T execute(@Nonnull V query)
            throws HttpConnectorException, InterruptedException {
        try {
            return httpConnectorFactory.getConnector(credentials).execute(query);
        } catch (HttpConnectorException e) {
            throw new AccessTokenRequestException(
                    "Failed to request access token from URL: " + credentials.getOAuthUrl(),
                    e.getErrorStatus().orElse(null), e.getRawResponse(), e);
        }
    }

    /**
     * close the factory.
     *
     * @throws IOException if failed to close the connector factory.
     */
    @Override
    public void close() throws IOException {
        if (httpConnectorFactory != null) {
            httpConnectorFactory.close();
        }
    }
}
