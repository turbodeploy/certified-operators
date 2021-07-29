package com.vmturbo.auth.api.securestorage;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.AUTH_HEADER_NAME;
import static com.vmturbo.auth.api.db.DBPasswordUtil.SECURESTORAGE_PATH;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.authorization.kvstore.IComponentJwtStore;
import com.vmturbo.communication.CommunicationException;

/**
 * The client for secure storage that is managed by auth component and used to store sensitive information. The auth
 * provides a rest API for storing values securely in it. This API requires authentication. This client facilitates
 * the making the rest call and doing authentication.
 */
public class SecureStorageClient {

    private static final List<MediaType> HTTP_ACCEPT = ImmutableList.of(MediaType.APPLICATION_JSON);

    /**
     * The host for auth http endpoint.
     */
    final String authHost;

    /**
     * The port for auth http endpoint.
     */
    final int authPort;

    final String authRoute;

    /**
     * The component JWT store.
     */
    final IComponentJwtStore componentJwtStore;

    /**
     * The synchronous client-side HTTP access.
     */
    private final RestTemplate restTemplate;

    /**
     * Creates an instance of client to interact with secure storage.
     *
     * @param authHost the host for auth service.
     * @param authPort the port auth service listens to.
     * @param authRoute the route that service listens on.
     * @param componentJwtStore the JWT store used for getting authentication information.
     */
    public SecureStorageClient(@Nonnull String authHost, int authPort,
                               @Nonnull String authRoute,
                               @Nonnull final IComponentJwtStore componentJwtStore) {
        this.authHost = Objects.requireNonNull(authHost);
        this.authPort = authPort;
        this.authRoute = Objects.requireNonNull(authRoute);
        this.componentJwtStore = Objects.requireNonNull(componentJwtStore);
        this.restTemplate = new RestTemplate();
    }

    /**
     * Get the value for a key to secure storage.
     *
     * @param subject the subject that the key is stored under.
     * @param key the key.
     * @return the value for the key.
     * @throws CommunicationException when there is an issue communicating with server.
     */
    public Optional<String> getValue(String subject, String key) throws CommunicationException {
        try {
            final ResponseEntity<String> result =
                    restTemplate.exchange(createUri("get", subject, key), HttpMethod.GET, getEntity(null),
                            String.class);
            if (result.getStatusCode() == HttpStatus.OK) {
                return Optional.of(result.getBody());
            } else {
                throw new CommunicationException("The auth server returned unexpected code " + result.getStatusCode());
            }
        } catch (RestClientException ex) {
            if (ex instanceof HttpStatusCodeException
                    && ((HttpStatusCodeException)ex).getStatusCode() == HttpStatus.BAD_REQUEST) {
                return Optional.empty();
            } else {
                throw new CommunicationException("Failed to access auth secure storage endpoint", ex);
            }
        }
    }

    /**
     * Adds/Updates the value for a key in secure storage.
     *
     * @param subject the subject that the key is stored under.
     * @param key the key.
     * @param value the value for the key.
     * @throws CommunicationException when there is an issue communicating with server.
     */
    public void updateValue(String subject, String key, String value) throws CommunicationException {
        try {
            final ResponseEntity<String> result =
                    restTemplate.exchange(createUri("modify", subject, key), HttpMethod.PUT, getEntity(value),
                            String.class);
            if (result.getStatusCode() != HttpStatus.OK) {
                throw new CommunicationException("The auth server returned unexpected code " + result.getStatusCode());
            }
        } catch (RestClientException ex) {
            throw new CommunicationException("Failed to access auth secure storage endpoint", ex);
        }
    }

    /**
     * Deletes the value for a key in secure storage.
     *
     * @param subject the subject that the key is stored under.
     * @param key the key.
     * @throws CommunicationException when there is an issue communicating with server.
     */
    public void deleteValue(String subject, String key) throws CommunicationException {
        try {
            final ResponseEntity<String> result =
                    restTemplate.exchange(createUri("delete", subject, key), HttpMethod.DELETE, getEntity(null),
                    String.class);
            if (result.getStatusCode() != HttpStatus.OK) {
                throw new CommunicationException("The auth server returned unexpected code " + result.getStatusCode());
            }
        } catch (RestClientException ex) {
            throw new CommunicationException("Failed to access auth secure storage endpoint", ex);
        }
    }

    private String createUri(String operation, String subject, String key) {
        return UriComponentsBuilder.newInstance()
                .scheme("http")
                .host(authHost)
                .port(authPort)
                .path(authRoute + SECURESTORAGE_PATH + String.join("/", operation, subject, key))
                .build().toUriString();
    }

    private HttpEntity<?> getEntity(String body) {
        final HttpHeaders headers = new HttpHeaders();
        headers.setAccept(HTTP_ACCEPT);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(AUTH_HEADER_NAME,
                componentJwtStore.generateToken().getCompactRepresentation());
        headers.set(SecurityConstant.COMPONENT_ATTRIBUTE, componentJwtStore.getNamespace());
        return new HttpEntity<>(body, headers);
    }
}
