package com.vmturbo.mediation.webhook.oauth;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.connector.common.HttpConnectorException;
import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.mediation.connector.common.ParametersBasedHttpBody;
import com.vmturbo.mediation.connector.common.http.HttpParameter;
import com.vmturbo.mediation.webhook.WebhookProperties;
import com.vmturbo.mediation.webhook.connector.WebhookCredentials;
import com.vmturbo.mediation.webhook.connector.WebhookException;
import com.vmturbo.platform.sdk.probe.DataStorageException;
import com.vmturbo.platform.sdk.probe.IProbeDataStoreEntry;
import com.vmturbo.platform.sdk.probe.IProbeDataStoreEntryUpdate;
import com.vmturbo.platform.sdk.probe.TargetOperationException;

/**
 * Class to manage and request access tokens.
 */
public class TokenManager {
    private final Logger logger = LogManager.getLogger(getClass());

    // The probe data object that is used to load existing access tokens
    private final IProbeDataStoreEntry<byte[]> accessTokenData;

    // For each access token key, there is a single 'lock' object
    private final Map<AccessTokenKey, Object> locks;
    // For each access token key, there is a single 'AccessTokenResponse' object
    private Map<AccessTokenKey, AccessTokenResponse> tokens;


    /**
     * TokenManager constructor.
     */
    public TokenManager() {
        this(null);
    }


    /**
     * TokenManager constructor.
     *
     * @param accessTokenData The object used to access current probe persistent data.
     */
    public TokenManager(@Nullable IProbeDataStoreEntry<byte[]> accessTokenData) {
        locks = new HashMap<>();
        this.accessTokenData = accessTokenData;
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
    public AccessTokenResponse requestAccessToken(@Nonnull OAuthCredentials credentials,
            boolean requestNewToken) throws HttpConnectorException, IOException, InterruptedException {
        loadAccessTokenDataIfNeeded();
        AccessTokenKey accessTokenKey = AccessTokenKey.create(credentials);
        Object lock = acquireDistinctLock(accessTokenKey);
        synchronized (lock) {
            logger.trace("Retrieving Access Token for request with fields: {}", accessTokenKey);
            AccessTokenResponse accessToken;
            if (!requestNewToken) {
                accessToken = tokens.get(accessTokenKey);
                if (accessToken == null || accessToken.hasExpired()) {
                    accessToken = requestNewAccessToken(credentials,
                            accessToken);
                }
            } else {
                accessToken = requestNewAccessToken(credentials, tokens.get(accessTokenKey));
            }
            return accessToken;
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
    public AccessTokenResponse requestAccessToken(@Nonnull OAuthCredentials credentials)
            throws HttpConnectorException, IOException, InterruptedException {
        return requestAccessToken(credentials, false);
    }

    /**
     * Request a new access token using by first a refresh token if present. If the
     * request fails, it falls back to using the implied grant type.
     *
     * <p>Requires locking/Caller must use appropriate locking.</p>
     *
     * @param credentials the oauth credentials.
     * @param previousAccessToken the current access token that maps to this oauth credentials.
     * @return A new access token.
     * @throws HttpConnectorException if failed to retrieve the access token.
     * @throws IOException if failed to close the connection.
     * @throws InterruptedException if the blocking call was interrupted (ex: probe is
     */
    private AccessTokenResponse requestNewAccessToken(@Nonnull OAuthCredentials credentials,
            @Nullable AccessTokenResponse previousAccessToken)
            throws HttpConnectorException, IOException, InterruptedException {
        if (previousAccessToken != null && previousAccessToken.getRefreshToken() != null) {
            try {
                return requestNewAccessTokenUsingRefreshToken(credentials, previousAccessToken.getRefreshToken());
            } catch (HttpConnectorException ex) {
                logger.info("Failed to get the new access token using refresh token. "
                        + "Falling back to getting access token using implied grant type "
                        + "(OAuth client: {})", credentials, ex);
                return requestNewAccessTokenUsingImpliedGrantType(credentials);
            }
        } else {
            return requestNewAccessTokenUsingImpliedGrantType(credentials);
        }
    }

    /**
     * Request a new access token using a known refresh token.
     *
     * <p>Requires locking/Caller must use appropriate locking.</p>
     *
     * @param credentials the oauth credentials.
     * @param refreshToken the refresh token.
     * @return A new access token.
     * @throws HttpConnectorException if failed to retrieve the access token.
     * @throws IOException if failed to close the connection.
     * @throws InterruptedException if the blocking call was interrupted (ex: probe is
     */
    @Nonnull
    private AccessTokenResponse requestNewAccessTokenUsingRefreshToken(@Nonnull OAuthCredentials credentials,
            @Nonnull String refreshToken)
            throws HttpConnectorException, IOException, InterruptedException {
        AccessTokenRequestBody accessTokenRequestBody = new AccessTokenRequestBody(credentials);
        AccessTokenResponse newAccessToken = executeAccessTokenQuery(credentials,
                accessTokenRequestBody.createBodyUsingRefreshToken(refreshToken));
        // new access token does not contain refresh token, must reuse the previous one.
        if (newAccessToken.getRefreshToken() == null) {
            newAccessToken = new AccessTokenResponse(newAccessToken.getAccessToken(),
                    newAccessToken.getTokenType(), newAccessToken.getExpiresIn(),
                    refreshToken, newAccessToken.getScope(),
                    newAccessToken.getError(), newAccessToken.getErrorDescription());
        }
        tokens.put(AccessTokenKey.create(credentials), newAccessToken);
        updatePersistentData(AccessTokenKey.create(credentials), newAccessToken);
        logger.trace("Received new access token using refresh token: {}", newAccessToken);
        return newAccessToken;
    }

    /**
     * Request a new access token using the implied grant type.
     *
     * <p>Requires locking/Caller must use appropriate locking.</p>
     *
     * @param credentials the oauth credentials.
     * @return A new access token.
     * @throws HttpConnectorException if failed to retrieve the access token.
     * @throws IOException if failed to close the connection.
     * @throws InterruptedException if the blocking call was interrupted (ex: probe is
     */
    @Nonnull
    private AccessTokenResponse requestNewAccessTokenUsingImpliedGrantType(@Nonnull OAuthCredentials credentials)
            throws HttpConnectorException, IOException, InterruptedException {
        AccessTokenRequestBody accessTokenRequestBody = new AccessTokenRequestBody(credentials);
        AccessTokenResponse newAccessToken = executeAccessTokenQuery(credentials,
                accessTokenRequestBody.createBodyUsingImpliedGrantType());
        tokens.put(AccessTokenKey.create(credentials), newAccessToken);
        updatePersistentData(AccessTokenKey.create(credentials), newAccessToken);
        logger.trace("Received new access token using implied grant type: {}", newAccessToken);
        return newAccessToken;
    }

    /**
     * Tries to get a new access token for input credentials.
     *
     * @param credentials the credentials for getting the access token.
     * @return true if a new access token was received.
     * @throws HttpConnectorException if failed to connect to the oAuthUrl endpoint.
     * @throws IOException if an IO exception happens while interacting with endpoint.
     * @throws InterruptedException if the blocking call was interrupted (ex: probe is
     *         shutting down).
     */
    public boolean renewAccessToken(@Nonnull OAuthCredentials credentials)
            throws HttpConnectorException, IOException, InterruptedException {
        AccessTokenKey accessTokenKey = AccessTokenKey.create(credentials);
        Object lock = acquireDistinctLock(accessTokenKey);
        synchronized (lock) {
            AccessTokenResponse currentAccessTokenResponse = tokens.get(accessTokenKey);
            AccessTokenResponse newAccessTokenResponse =
                    requestNewAccessToken(credentials, currentAccessTokenResponse);
            return !Objects.equals(newAccessTokenResponse.getAccessToken(),
                    currentAccessTokenResponse.getAccessToken());
        }
    }

    /**
     * Attempts to request authorizing token from specified oAuth endpoint using
     * {@link AccessTokenConnector}.
     *
     * @param credentials the {@link OAuthCredentials} used to construct the request.
     * @param accessTokenParameters the body used to construct the http request.
     * @return the {@link AccessTokenResponse} from the authorizing server.
     * @throws HttpConnectorException if failed to connect to the oAuthUrl endpoint.
     * @throws IOException if failed to close the connection.
     * @throws InterruptedException if the blocking call was interrupted (ex: probe is
     *         shutting down).
     */
    private AccessTokenResponse executeAccessTokenQuery(OAuthCredentials credentials,
            Map<HttpParameter, String> accessTokenParameters)
            throws HttpConnectorException, IOException, InterruptedException {
        try (AccessTokenConnector accessTokenConnector = new AccessTokenConnector(credentials,
                WebhookProperties.CONNECTION_TIMEOUT_MSEC.getDefaultValue())) {
            AccessTokenQuery accessTokenQuery = new AccessTokenQuery(HttpMethodType.POST,
                    new ParametersBasedHttpBody(accessTokenParameters), Collections.emptyList());
            return accessTokenConnector.execute(accessTokenQuery);
        }
    }

    /**
     * Returns a lock associated with a {@link AccessTokenKey}. A new lock will
     * be created if one does not exist.
     *
     * @param accessTokenKey the accessTokenKey.
     * @return the object lock.
     */
    private Object acquireDistinctLock(@Nonnull AccessTokenKey accessTokenKey) {
        Object lock;
        synchronized (locks) {
            Object l = locks.get(accessTokenKey);
            if (l == null) {
                lock = new Object();
                locks.put(accessTokenKey, lock);
            } else {
                lock = l;
            }
        }
        return lock;
    }

    private void updatePersistentData(@Nonnull AccessTokenKey accessTokenKey,
                                      final AccessTokenResponse accessTokenResponse) throws InterruptedException {
        if (accessTokenData != null) {
            try {
                accessTokenData.compute(new WebhookProbeDataStoreEntryUpdate(accessTokenKey,
                        accessTokenResponse));
            } catch (TargetOperationException | TimeoutException | DataStorageException ex) {
                logger.error("Could not update access token cache for WebhookProbe.", ex);
            }
        }
    }

    private synchronized void loadAccessTokenDataIfNeeded() throws InterruptedException {
        if (tokens != null) {
            return;
        }

        if (accessTokenData != null) {
            try {
                Optional<byte[]> bytes = accessTokenData.load();
                if (bytes.isPresent()) {
                    tokens = deserialize(bytes.get());
                } else {
                    tokens = Maps.newConcurrentMap();
                }
            } catch (DataStorageException | IOException | ClassNotFoundException
                    | ClassCastException e) {
                logger.error("Failed to deserialize access token data for Webhook probe.", e);
                tokens = Maps.newConcurrentMap();
            }
        } else {
            tokens = Maps.newConcurrentMap();
        }
    }

    private static Map<AccessTokenKey, AccessTokenResponse> deserialize(byte[] bytes)
            throws IOException, ClassNotFoundException {
        final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        final ObjectInput in = new ObjectInputStream(bis);
        final Object obj = in.readObject();
        return (Map<AccessTokenKey, AccessTokenResponse>)obj;
    }

    @Nonnull
    private static byte[] serialize(Map<AccessTokenKey, AccessTokenResponse> table)
            throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(table);
        return bos.toByteArray();
    }

    /**
     * Class used to update the access tokens.
     */
    @VisibleForTesting
    public static class WebhookProbeDataStoreEntryUpdate implements
            IProbeDataStoreEntryUpdate<byte[]> {
        Logger logger = LogManager.getLogger();

        private final AccessTokenKey accessTokenKey;
        private final AccessTokenResponse response;

        WebhookProbeDataStoreEntryUpdate(AccessTokenKey accessTokenKey,
                                         AccessTokenResponse response) {
            this.accessTokenKey = accessTokenKey;
            this.response = response;
        }

        /**
         * The method that is called to update the value for persistent probe data.
         *
         * <p>Atomically stores the data object so it will be accessible by probe operations
         * across life cycles.</p>
         *
         * <p>This operation should block other data updates on the same target + feature category,
         * while updater is executing.</p>
         *
         * @param previous the previous value for the probe persistent data.
         * @return the new value for probe persistent data.
         */
        @Override
        @Nonnull
        public Optional<byte[]> update(Optional<byte[]> previous) {
            // since the copy of the access token table we have here maybe outdated. We don't
            // replace the persistent data table with current one. Instead, we use the table
            // in the call and add the new value to it. The caller ensures that not two update
            // calls to be run at the same time. There will be no race condition to update
            // the table.
            Map<AccessTokenKey, AccessTokenResponse> currentTable;
            if (previous.isPresent()) {
                try {
                    currentTable = deserialize(previous.get());
                } catch (IOException | ClassNotFoundException | ClassCastException e) {
                    logger.error("Failed to deserialize previous value.", e);
                    currentTable = Maps.newConcurrentMap();
                }
            } else {
                currentTable = Maps.newConcurrentMap();
            }
            currentTable.put(accessTokenKey, response);
            try {
                final byte[] serialized = serialize(currentTable);
                return Optional.of(serialized);
            } catch (IOException ex) {
                logger.error("Failed to serialize current access token map.", ex);
                return previous;
            }
        }
    }
}
