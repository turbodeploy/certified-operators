package com.vmturbo.external.api;

import java.net.URI;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import feign.Feign;
import feign.RequestInterceptor;
import feign.RequestTemplate;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;

/**
 * A client class to use to connect to the external Turbonomic API.
 *
 * Create a {@link TurboApiClient} using {@link TurboApiClient#newBuilder()}, and
 * get stubs for the appropriate API class using {@link TurboApiClient#getStub(Class)}.
 */
public class TurboApiClient {

    private final Logger logger = LogManager.getLogger();

    private volatile boolean loggedIn = false;

    private final Feign.Builder feignBuilder = Feign.builder()
            .encoder(new JacksonEncoder())
            .decoder(new JacksonDecoder());

    private final String host;

    private final int port;

    private final String user;

    private final String password;

    private TurboApiClient(@Nonnull final String host,
                          final int port,
                          @Nonnull final String user,
                          @Nonnull final String password) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    /**
     * Create a new builder for a {@link TurboApiClient}.
     *
     * @return The {@link Builder}.
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Create a stub for an API class. The stub will use this API client for the connection.
     *
     * @param stubType The class of the stub.
     * @param <StubClass> The class of the stub.
     * @return The new stub.
     */
    @Nonnull
    public <StubClass> StubClass getStub(Class<StubClass> stubType) {
        ensureLogin();
        return feignBuilder.target(stubType, baseUri().toUriString());
    }

    /**
     * A builder for {@link TurboApiClient}.
     */
    public static class Builder {
        private String host = "localhost";
        private int port = 80;
        private String user = "administrator";
        private String password = "admin";

        public Builder setPassword(final String password) {
            this.password = password;
            return this;
        }

        public Builder setUser(final String user) {
            this.user = user;
            return this;
        }

        public Builder setPort(final int port) {
            this.port = port;
            return this;
        }

        public Builder setHost(final String host) {
            this.host = host;
            return this;
        }

        public TurboApiClient build() {
            return new TurboApiClient(host, port, user, password);
        }
    }

    @Nonnull
    private UriComponentsBuilder baseUri() {
        return UriComponentsBuilder.newInstance()
                .scheme("http")
                .host(host)
                .port(port)
                .pathSegment("vmturbo", "rest");
    }

    /**
     * Ensure that the client is logged in with the username and password it was constructed with.
     * <p>
     * If the administrator user hasn't been initialized yet, this method creates an administrator
     * with the username and password.
     */
    private synchronized void ensureLogin() {
        if (loggedIn) {
            return;
        }

        final RestTemplate restTemplate = new RestTemplate();
        final boolean initialized = restTemplate.exchange(
                RequestEntity.get(baseUri().path("checkInit").build().toUri()).build(),
                Boolean.class)
            .getBody();
        if (!initialized) {
            logger.info("Initializing administrator user.");
            final URI uri = baseUri().path("initAdmin")
                    .queryParam("username", user)
                    .queryParam("password", password)
                    .build().encode().toUri();
            restTemplate.exchange(RequestEntity.post(uri).build(), Void.class);
        }

        final URI uri = baseUri().path("login")
                .queryParam("username", user)
                .queryParam("password", password)
                .queryParam("remember", "true").build().encode().toUri();

        final ResponseEntity<?> retDto =
                restTemplate.exchange(RequestEntity.post(uri).build(), Void.class);

        final String sessionId = retDto.getHeaders().get("Set-Cookie").stream()
                .flatMap(str -> Stream.of(str.split(";")))
                .filter(str -> str.startsWith("JSESSIONID"))
                .map(str -> str.split("=")[1]).findFirst().get();

        logger.info("Session ID: {}", sessionId);

        final AuthRequestInterceptor interceptor = new AuthRequestInterceptor(sessionId);
        feignBuilder.requestInterceptor(interceptor);

        loggedIn = true;
    }

    /**
     * A {@link RequestInterceptor} that adds the JSESSIONID of the current session to all requests.
     * <p>
     */
    private static class AuthRequestInterceptor implements RequestInterceptor {
        private final String sessionId;

        private AuthRequestInterceptor(final String sessionId) {
            this.sessionId = sessionId;
        }

        @Override
        public void apply(final RequestTemplate template) {
            template.header("Cookie", "JSESSIONID=" + sessionId);
        }
    }
}
