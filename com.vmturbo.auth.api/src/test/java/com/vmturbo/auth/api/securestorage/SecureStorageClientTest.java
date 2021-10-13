package com.vmturbo.auth.api.securestorage;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.authorization.kvstore.IComponentJwtStore;
import com.vmturbo.communication.CommunicationException;

/**
 * Test the class {@link SecureStorageClient}.
 */
public class SecureStorageClientTest {
    private static final String VALUE = "testValue";
    private static final String SUBJECT = "testSubject";
    private static final String KEY = "testKey";
    private static final String JWT_TOKEN = "TestToken";

    private static final int SERVER_PORT = 28122;
    private static final String AUTH_ROUTE = "auth";

    private static CachingHttpHandler handler;
    private static HttpServer server;
    private static ThreadPoolExecutor executor;

    private static final Logger logger = LogManager.getLogger();

    @Mock
    private IComponentJwtStore componentJwtStore;

    @Mock
    private JWTAuthorizationToken authorizationToken;

    private SecureStorageClient secureStorageClient;

    /**
     * Performs initializations before running the test.
     */
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        handler.resetHandler();
        when(componentJwtStore.generateToken()).thenReturn(authorizationToken);
        when(authorizationToken.getCompactRepresentation()).thenReturn(JWT_TOKEN);
        secureStorageClient = new SecureStorageClient("localhost", SERVER_PORT, AUTH_ROUTE, componentJwtStore);
    }

    /**
     * Setup an http server for test.
     *
     * @throws IOException if port cannot get claimed
     */
    @BeforeClass
    public static void setupServer() throws IOException {
        handler = new CachingHttpHandler();
        server = HttpServer.create(new InetSocketAddress("localhost", SERVER_PORT), 0);
        executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(1);
        server.createContext("/", handler);
        server.setExecutor(executor);
        server.start();
        logger.info("Server started on port " + SERVER_PORT);
    }

    /**
     * Shuts down the server and threadpool.
     */
    @AfterClass
    public static void shutdownServer() {
        logger.info(" Shutting down server started on port " + SERVER_PORT);
        server.stop(0);
        executor.shutdownNow();
    }

    /**
     * Tests a scenario that client that gets a value.
     *
     * @throws CommunicationException if something goes wrong connecting to server.
     */
    @Test
    public void testGetValue() throws CommunicationException {
        // ARRANGE
        handler.setResponseBody(VALUE);

        // ACT
        Optional<String> value = secureStorageClient.getValue(SUBJECT, KEY);

        // ASSERT
        assertTrue(value.isPresent());
        assertThat(value.get(), equalTo(VALUE));
        assertTrue(handler.getLastRequest().isPresent());
        assertThat(handler.getLastRequest().get().method, equalTo("GET"));
        assertThat(handler.getLastRequest().get().url, equalTo("/auth/securestorage/get/testSubject/testKey"));
        assertThat(handler.getLastRequest().get().payload, equalTo(null));
        assertThat(handler.getLastRequest().get().headers.get(SecurityConstant.AUTH_HEADER_NAME),
                equalTo(Collections.singletonList(JWT_TOKEN)));
    }

    /**
     * Tests a scenario that client gets a value and it is not found.
     *
     * @throws CommunicationException if something goes wrong connecting to server.
     */
    @Test
    public void testGetValueNotFound() throws CommunicationException {
        // ARRANGE
        handler.setResponseCode(HttpStatus.SC_BAD_REQUEST);

        // ACT
        Optional<String> value = secureStorageClient.getValue(SUBJECT, KEY);

        // ASSERT
        assertFalse(value.isPresent());
        assertTrue(handler.getLastRequest().isPresent());
        assertThat(handler.getLastRequest().get().method, equalTo("GET"));
        assertThat(handler.getLastRequest().get().url, equalTo("/auth/securestorage/get/testSubject/testKey"));
        assertThat(handler.getLastRequest().get().payload, equalTo(null));
        assertThat(handler.getLastRequest().get().headers.get(SecurityConstant.AUTH_HEADER_NAME),
                equalTo(Collections.singletonList(JWT_TOKEN)));
    }

    /**
     * Tests a scenario that client gets a value and the server returns unexpected code.
     *
     * @throws CommunicationException if something goes wrong connecting to server.
     */
    @Test(expected = CommunicationException.class)
    public void testGetValueUnexpectedCode() throws CommunicationException {
        // ARRANGE
        handler.setResponseCode(HttpStatus.SC_CREATED);

        // ACT
        secureStorageClient.getValue(SUBJECT, KEY);
    }

    /**
     * Test a scenario that client updates a value.
     *
     * @throws CommunicationException if something goes wrong connecting to server.
     */
    @Ignore("This test is failing once in the while. It is disabled until we figure out why (bug: OM-76350)")
    @Test
    public void testUpdateValue() throws CommunicationException {
        // ARRANGE

        // ACT
        secureStorageClient.updateValue(SUBJECT, KEY, VALUE);

        // ASSERT
        assertTrue(handler.getLastRequest().isPresent());
        assertThat(handler.getLastRequest().get().method, equalTo("PUT"));
        assertThat(handler.getLastRequest().get().url, equalTo("/auth/securestorage/modify/testSubject/testKey"));
        assertThat(handler.getLastRequest().get().payload, equalTo(VALUE));
        assertThat(handler.getLastRequest().get().headers.get(SecurityConstant.AUTH_HEADER_NAME),
                equalTo(Collections.singletonList(JWT_TOKEN)));
    }

    /**
     * Gets a scenario that the client updates a value and internal error happens.
     *
     * @throws CommunicationException if something goes wrong connecting to server.
     */
    @Test(expected = CommunicationException.class)
    public void testUpdateValueInternalError() throws CommunicationException {
        // ARRANGE
        handler.setResponseCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);

        // ACT
        secureStorageClient.updateValue(SUBJECT, KEY, VALUE);
    }

    /**
     * Test a scenario that client deletes a value.
     *
     * @throws CommunicationException if something goes wrong connecting to server.
     */
    @Test
    public void testDeleteValue() throws CommunicationException {
        // ARRANGE

        // ACT
        secureStorageClient.deleteValue(SUBJECT, KEY);

        // ASSERT
        assertTrue(handler.getLastRequest().isPresent());
        assertThat(handler.getLastRequest().get().method, equalTo("DELETE"));
        assertThat(handler.getLastRequest().get().url, equalTo("/auth/securestorage/delete/testSubject/testKey"));
        assertThat(handler.getLastRequest().get().payload, equalTo(null));
        assertThat(handler.getLastRequest().get().headers.get(SecurityConstant.AUTH_HEADER_NAME),
                equalTo(Collections.singletonList(JWT_TOKEN)));
    }

    /**
     * Test a scenario that client deletes a value and internal error happens.
     *
     * @throws CommunicationException if something goes wrong connecting to server.
     */
    @Test(expected = CommunicationException.class)
    public void testDeleteValueInternalError() throws CommunicationException {
        // ARRANGE
        handler.setResponseCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);

        // ACT
        secureStorageClient.deleteValue(SUBJECT, KEY);
    }

    /**
     * A response handler that caches the last request made to it.
     */
    private static class CachingHttpHandler implements HttpHandler {
        private Request lastRequest = null;
        private int responseCode = 200;
        private String responseBody = null;

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            final String body;
            if (exchange.getRequestBody().available() != 0) {
                body = IOUtils.toString(exchange.getRequestBody(), StandardCharsets.UTF_8.name());
            } else {
                body = null;
            }

            lastRequest = new Request(exchange.getRequestMethod(), exchange.getRequestURI().toString(), body,
                    exchange.getRequestHeaders());

            if (responseBody != null) {
                final byte[] response = responseBody.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(responseCode, response.length);
                exchange.getResponseBody().write(response);
            } else {
                exchange.sendResponseHeaders(responseCode, 0);
            }
            exchange.getResponseBody().close();
        }

        void setResponseCode(int responseCode) {
            this.responseCode = responseCode;
        }

        void setResponseBody(String responseBody) {
            this.responseBody = responseBody;
        }

        Optional<Request> getLastRequest() {
            return Optional.ofNullable(lastRequest);
        }

        void resetHandler() {
            lastRequest = null;
            responseBody = null;
            responseCode = 200;
        }
    }

    /**
     * Keeps information about an http request.
     */
    private static class Request {
        String method;
        String url;
        String payload;
        Map<String, List<String>> headers;

        Request(String method, String url, String payload, Map<String, List<String>> headers) {
            this.method = method;
            this.url = url;
            this.payload = payload;
            this.headers = headers;
        }
    }
}