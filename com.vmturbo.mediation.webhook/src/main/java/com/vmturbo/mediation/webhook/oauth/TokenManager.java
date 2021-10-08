package com.vmturbo.mediation.webhook.oauth;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;

import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.mediation.connector.common.ParametersBasedHttpBody;
import com.vmturbo.mediation.connector.common.http.HttpParameter;
import com.vmturbo.mediation.webhook.WebhookProperties;
import com.vmturbo.mediation.webhook.connector.WebhookCredentials;
import com.vmturbo.mediation.webhook.connector.WebhookException;

/**
 * Class to manage and request access tokens.
 */
public class TokenManager {
    // For each unique oAuthUrl and ClientID pair, there is a single 'lock' object
    private final Table<String, String, Object> locks;
    // For each unique oAuthUrl and ClientID pair, there is a single 'AccessTokenResponse' object
    private final Table<String, String, AccessTokenResponse> tokens;

    /**
     * TokenManager constructor.
     */
    public TokenManager() {
        locks = HashBasedTable.create();
        tokens = Tables.synchronizedTable(HashBasedTable.create());
    }

    /**
     * Retrieves an {@link AccessTokenResponse}.
     *
     * @param credentials the {@link WebhookCredentials} used to construct the request.
     * @param requestNewToken a boolean to indicate if a new request should be made to retrieve
     * a new token.
     * @return the {@link AccessTokenResponse} from the authorizing server.
     * @throws WebhookException if failed to connect to the oAuthUrl endpoint.
     * @throws IOException if failed to close the connection.
     * @throws InterruptedException if the blocking call was interrupted (ex: probe is
     *         shutting down).
     */
    public AccessTokenResponse requestAccessToken(@Nonnull WebhookCredentials credentials,
            boolean requestNewToken) throws WebhookException, IOException, InterruptedException {
        String url = credentials.getOAuthUrl();
        String clientId = credentials.getClientID();
        Object lock = acquireDistinctUrlClientIdLock(url, clientId);
        synchronized (lock) {
            AccessTokenResponse accessTokenResponse;
            if (!requestNewToken) {
                accessTokenResponse = tokens.get(url, clientId);
                if (accessTokenResponse == null || accessTokenResponse.hasExpired()) {
                    accessTokenResponse = retrieveAccessToken(credentials);
                    tokens.put(url, clientId, accessTokenResponse);
                }
            } else {
                accessTokenResponse = retrieveAccessToken(credentials);
                tokens.put(url, clientId, accessTokenResponse);
            }
            return accessTokenResponse;
        }
    }

    /**
     * Retrieves an {@link AccessTokenResponse}, using cached token if present.
     *
     * @param credentials the {@link WebhookCredentials} used to construct the request.
     * @return the {@link AccessTokenResponse} from the authorizing server.
     * @throws WebhookException if failed to connect to the oAuthUrl endpoint.
     * @throws IOException if failed to close the connection.
     * @throws InterruptedException if the blocking call was interrupted (ex: probe is
     *         shutting down).
     */
    public AccessTokenResponse requestAccessToken(@Nonnull WebhookCredentials credentials)
            throws WebhookException, IOException, InterruptedException {
        return requestAccessToken(credentials, false);
    }

    /**
     * Attempts to request authorizing token from specified oAuth endpoint using
     * {@link AccessTokenConnector}.
     *
     * @param credentials the {@link WebhookCredentials} used to construct the request.
     * @return the {@link AccessTokenResponse} from the authorizing server.
     * @throws WebhookException if failed to connect to the oAuthUrl endpoint.
     * @throws IOException if failed to close the connection.
     * @throws InterruptedException if the blocking call was interrupted (ex: probe is
     *         shutting down).
     */
    private AccessTokenResponse retrieveAccessToken(WebhookCredentials credentials)
            throws WebhookException, IOException, InterruptedException {
        try (AccessTokenConnector accessTokenConnector = new AccessTokenConnector(credentials,
                WebhookProperties.CONNECTION_TIMEOUT_MSEC.getDefaultValue())) {
            Map<HttpParameter, String> accessTokenParameters =
                    new HashMap<HttpParameter, String>() {{
                        put(OAuthParameters.CLIENT_ID, credentials.getClientID());
                        put(OAuthParameters.CLIENT_SECRET, credentials.getClientSecret());
                        put(OAuthParameters.GRANT_TYPE, credentials.getGrantType().getValue());
                        if (credentials.getScope() != null) {
                            put(OAuthParameters.SCOPE, credentials.getScope());
                        }
                    }};
            AccessTokenQuery accessTokenQuery = new AccessTokenQuery(HttpMethodType.POST,
                    new ParametersBasedHttpBody(accessTokenParameters), Collections.emptyList());
            return accessTokenConnector.execute(accessTokenQuery);
        }
    }

    /**
     * Returns a lock associated with a oAuthUrl-clientId pair. A new lock will
     * be created if one does not exist.
     *
     * @param url The oAuthUrl.
     * @param clientId The clientId.
     * @return the object lock for the unique oAuthUrl-clientId pairing.
     */
    private Object acquireDistinctUrlClientIdLock(String url, String clientId) {
        Object lock;
        synchronized (locks) {
            Object l = locks.get(url, clientId);
            if (l == null) {
                lock = new Object();
                locks.put(url, clientId, lock);
            } else {
                lock = l;
            }
        }
        return lock;
    }
}
