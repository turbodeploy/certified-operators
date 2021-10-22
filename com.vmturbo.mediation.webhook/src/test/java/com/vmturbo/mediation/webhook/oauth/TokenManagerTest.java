package com.vmturbo.mediation.webhook.oauth;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

import com.google.common.collect.Sets;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.mediation.webhook.connector.WebhookCredentials;
import com.vmturbo.mediation.webhook.connector.WebhookException;
import com.vmturbo.platform.sdk.common.util.WebhookConstants.AuthenticationMethod;

/**
 * Class to test TokenManager.
 */
public class TokenManagerTest {

    private static HttpServer server;
    private static ThreadPoolExecutor executor;

    private final WebhookCredentials credentials = new WebhookCredentials("http://:142/endpoint",
            HttpMethodType.POST.name(), 30000L, AuthenticationMethod.BASIC, null, null, false,
            "http://localhost:28121", "abc", "123", GrantType.CLIENT_CREDENTIALS, "wide");

    /**
     * Setup an http server for test.
     *
     * @throws IOException if port cannot get claimed
     */
    @BeforeClass
    public static void setupServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 28121), 0);
        executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(5);
        server.createContext("/", generateAccessTokenHandler);
        server.createContext("/expired", generateExpiredAccessTokenHandler);
        server.createContext("/unexpected-json", generateUnexpectedJsonHandler);
        server.setExecutor(executor);
        server.start();
    }

    /**
     * Test that multiple requests using the same oAuthUrl-clientId pair does not
     * generate new tokens.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testAccessTokenRequestIsCached() throws Exception {
        TokenManager tokenManager = new TokenManager();
        AccessTokenResponse firstRequest = tokenManager.requestAccessToken(credentials);
        AccessTokenResponse secondRequest = tokenManager.requestAccessToken(credentials);
        Assert.assertEquals(firstRequest, secondRequest);
    }

    /**
     * Test that requesting a new (non cached) token request does return a new token.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testNewAccessTokenRequest() throws Exception {
        TokenManager tokenManager = new TokenManager();
        AccessTokenResponse firstRequest = tokenManager.requestAccessToken(credentials);
        AccessTokenResponse secondRequest = tokenManager.requestAccessToken(credentials, true);
        Assert.assertNotEquals(firstRequest, secondRequest);
    }

    /**
     * Tests that an access token is expired.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testAccessTokenExpiry() throws Exception {
        TokenManager tokenManager = new TokenManager();
        WebhookCredentials credentials = new WebhookCredentials("http://:142/endpoint",
                HttpMethodType.POST.name(), 30000L, AuthenticationMethod.BASIC, null, null, false,
                "http://localhost:28121/expired", "abc", "123", GrantType.CLIENT_CREDENTIALS, "wide");
        AccessTokenResponse accessTokenResponse = tokenManager.requestAccessToken(credentials);
        Assert.assertTrue(accessTokenResponse.hasExpired());
    }

    /**
     * Tests that no exception is thrown when an unexpected json response is returned.
     */
    @Test
    public void testNoExceptionIsThrownForUnexpectedJson() {
        AccessTokenResponse accessTokenResponse = null;
        try {
            TokenManager tokenManager = new TokenManager();
            WebhookCredentials credentials = new WebhookCredentials("http://:142/endpoint",
                    HttpMethodType.POST.name(), 30000L, AuthenticationMethod.BASIC, null, null, false,
                    "http://localhost:28121/unexpected-json", "abc", "123", GrantType.CLIENT_CREDENTIALS, "wide");
            accessTokenResponse = tokenManager.requestAccessToken(credentials);
        } catch (Exception e) {
            // exception should not be thrown
            fail();
        }

        Assert.assertNotNull(accessTokenResponse);
        Assert.assertNull(accessTokenResponse.getAccessToken());
        Assert.assertNull(accessTokenResponse.getRefreshToken());
        Assert.assertNull(accessTokenResponse.getTokenType());
        Assert.assertNull(accessTokenResponse.getError());
        Assert.assertNull(accessTokenResponse.getErrorDescription());
    }

    /**
     * Tests that concurrent requests for a token do not generate multiple {@link AccessTokenResponse}
     * for a single oAuthUrl-clientId pair.
     * @throws Exception should not be thrown.
     */
    @Test
    public void testAccessTokenRequestConcurrency() throws Exception {
        final TokenManager tokenManager = new TokenManager();
        final Set<String> tokens = Sets.newConcurrentHashSet();
        ExecutorService executorService = Executors.newCachedThreadPool();
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            tasks.add(() -> {
                try {
                    String token = tokenManager.requestAccessToken(credentials)
                            .getAccessToken();
                    tokens.add(token);
                } catch (WebhookException | InterruptedException | IOException e) {
                    e.printStackTrace();
                }
                return null;
            });
        }
        executorService.invokeAll(tasks);
        Assert.assertEquals(tokens.size(), 1);
    }

    /**
     * Test exception is thrown when attempting to access endpoint that does not exist.
     *
     * @throws WebhookException should be thrown when trying failed to access endpoint.
     * @throws IOException should not be thrown.
     * @throws InterruptedException should not be thrown.
     */
    @Test(expected = WebhookException.class)
    public void testRequestAccessTokenToInvalidEndpoint()
            throws WebhookException, IOException, InterruptedException {
        TokenManager tokenManager = new TokenManager();
        WebhookCredentials credentials = new WebhookCredentials("http://fake_webhook:142/endpoint",
                HttpMethodType.POST.name(), 30000L, AuthenticationMethod.BASIC, null, null, false,
                "http://notvalid:28121", "abc", "123", GrantType.CLIENT_CREDENTIALS, "wide");
        tokenManager.requestAccessToken(credentials);
    }

    /**
     * Generic HttpHandler which accepts a string representing the http response body.
     */
    private static final Function<String, HttpHandler> httpResponseBodyHandler = responseBody -> exchange ->  {
        final byte[] response = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
        exchange.getResponseBody().close();
    };

    /**
     * Endpoint to generate new access token response.
     */
    private static final HttpHandler generateAccessTokenHandler = httpResponseBodyHandler.apply(
            generateResponseBody(RandomStringUtils.random(10, "UTF-8"), "bearer", "3600", "123",
                    "wide"));

    /**
     * Endpoint to generate expired Access token response.
     */
    private static final HttpHandler generateExpiredAccessTokenHandler =
            httpResponseBodyHandler.apply(
                    generateResponseBody(RandomStringUtils.random(10, "UTF-8"), "bearer", "0",
                            "123", "wide"));

    /**
     * Endpoint to generate valid json that does not provide any fields for
     * {@link AccessTokenResponse}.
     */
    private static final HttpHandler generateUnexpectedJsonHandler = httpResponseBodyHandler.apply(
            "{\n" + "   \"data\":\"unexpected\"\n" + "}");

    /**
     * Helper method to generate json formatted access token response.
     *
     * @param accessToken the access token string.
     * @param tokenType the access token type.
     * @param expiresIn the time from now, in ms, when the token expires.
     * @param refreshToken the refresh token.
     * @param scope the scope
     * @return json formatted string response.
     */
    private static String generateResponseBody(String accessToken, String tokenType,
            String expiresIn, String refreshToken, String scope) {
        return "{\n" + "  \"access_token\":\"" + accessToken + "\",\n" + "  \"token_type\":\""
                + tokenType + "\",\n" + "  \"expires_in\":" + expiresIn + ",\n"
                + "  \"refresh_token\":\"" + refreshToken + "\",\n" + "  \"scope\":\"" + scope
                + "\"\n" + "}";
    }
}
