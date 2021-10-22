package com.vmturbo.mediation.webhook;

import static com.vmturbo.mediation.webhook.WebhookProbeTest.createWorkflow;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

import com.google.common.collect.Sets;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.mediation.webhook.WebhookProbeTest.WebhookProperties.WebhookPropertiesBuilder;
import com.vmturbo.mediation.webhook.oauth.AccessTokenKey;
import com.vmturbo.mediation.webhook.oauth.AccessTokenResponse;
import com.vmturbo.mediation.webhook.oauth.GrantType;
import com.vmturbo.mediation.webhook.oauth.OAuthParameters;
import com.vmturbo.mediation.webhook.oauth.TokenManager;
import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.sdk.common.util.WebhookConstants;
import com.vmturbo.platform.sdk.common.util.WebhookConstants.AuthenticationMethod;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProbeDataStoreEntry;
import com.vmturbo.platform.sdk.probe.IProgressTracker;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Test running actions against a http server that is running locally.
 */
public class WebhookProbeLocalServerTest {
    private static final long ACTION_CREATION_TIME = 1609367961000L;
    private static final long ACTION_UPDATE_TIME = 1609367994000L;
    private static final String ACCEPTED_BY = "administrator(3391249354768)";
    private static final long ACTION_UUID = 144151046183109L;
    private static final long VM_UUID = 23525323L;
    private static final String VM_LOCAL_NAME = "vm-27442";
    private static final String VM_DISPLAY_NAME = "turbonomic-t8c";
    private static final String TARGET_ADDRESS = "hp-dl365.corp.vmturbo.com";
    private static final String RESIZE_ACTION_DESC = "Resize up VCPU for Virtual Machine turbonomic-t8c from 2 to 3";
    private static final String MOVE_RISK_DESC = "Q4 VCPU, Mem Congestion";
    private static final int OLD_CPU_CAPACITY = 2;
    private static final int NEW_CPU_CAPACITY = 3;
    private static final String SERVER_URL = "http://localhost:28121";
    private static final String GET_METHOD_PATH = "/get/method";
    private static final String OAUTH_PATH = "/oauth";
    private static final String OAUTH_URL = SERVER_URL + OAUTH_PATH;
    private static final String CLIENT_ID = "test_client_id";
    private static final String CLIENT_SECRET = "test_client_secret";
    private static final String SCOPE = "test_scope";
    private static final String ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6I";
    private static final String SECOND_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI14";
    private static final String OAUTH_RESPONSE = "{\n"
            + "  \"token_type\": \"Bearer\",\n"
            + "  \"expires_in\": 3599,\n"
            + "  \"access_token\": \"%s\"}";

    private static final ActionExecutionDTO ON_PREM_RESIZE_ACTION =
            ActionExecutionDTO.newBuilder()
                    .setActionOid(ACTION_UUID)
                    .setActionState(ActionResponseState.IN_PROGRESS)
                    .setCreateTime(ACTION_CREATION_TIME)
                    .setUpdateTime(ACTION_UPDATE_TIME)
                    .setAcceptedBy(ACCEPTED_BY)
                    .setActionType(ActionItemDTO.ActionType.RESIZE)
                    .addActionItem(ActionItemDTO.newBuilder()
                            .setActionType(ActionItemDTO.ActionType.RESIZE)
                            .setUuid(String.valueOf(ACTION_UUID))
                            .setTargetSE(CommonDTO.EntityDTO.newBuilder()
                                    .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE)
                                    .setId("Test1")
                                    .setTurbonomicInternalId(VM_UUID)
                                    .setDisplayName(VM_DISPLAY_NAME)
                                    .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                                            .setNamespace("DEFAULT")
                                            .setName("LocalName")
                                            .setValue(VM_LOCAL_NAME)
                                            .build())
                                    .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                                            .setNamespace("DEFAULT")
                                            .setName("targetAddress")
                                            .setValue(TARGET_ADDRESS)
                                            .build())
                                    .setPowerState(CommonDTO.EntityDTO.PowerState.POWERED_ON))
                            .setCurrentComm(CommonDTO.CommodityDTO.newBuilder()
                                    .setCommodityType(CommonDTO.CommodityDTO.CommodityType.VCPU)
                                    .setCapacity(OLD_CPU_CAPACITY)
                                    .build())
                            .setNewComm(CommonDTO.CommodityDTO.newBuilder()
                                    .setCommodityType(CommonDTO.CommodityDTO.CommodityType.VCPU)
                                    .setCapacity(NEW_CPU_CAPACITY)
                                    .build())
                            .setDescription(RESIZE_ACTION_DESC)
                            .setRisk(ActionItemDTO.Risk.newBuilder()
                                    .setSeverity(ActionItemDTO.Risk.Severity.CRITICAL)
                                    .setCategory(ActionItemDTO.Risk.Category.PERFORMANCE_ASSURANCE)
                                    .setDescription(MOVE_RISK_DESC)
                                    .addAffectedCommodity(CommonDTO.CommodityDTO.CommodityType.VCPU)
                            )
                    )
                    .build();


    private static CachingHttpHandler handler;
    private static HttpServer server;
    private static ThreadPoolExecutor executor;
    private static WebhookProbe probe;
    private static IProgressTracker progressTracker;

    private static final Logger logger = LogManager.getLogger();

    private final WebhookAccount account = new WebhookAccount();
    private IProbeDataStoreEntry probePersistentData;

    /**
     * Setup an http server for test.
     *
     * @throws IOException if port cannot get claimed
     */
    @BeforeClass
    public static void setupServer() throws IOException {
        handler = new CachingHttpHandler();
        server = HttpServer.create(new InetSocketAddress("localhost", 28121), 0);
        executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(1);
        server.createContext("/not/found", exchange -> {
            exchange.sendResponseHeaders(404, 0);
            exchange.getResponseBody().close();
        });
        server.createContext("/", handler);
        server.setExecutor(executor);
        server.start();
        logger.info("Server started on port 28121");

    }

    /**
     * Utility method to help serve http-response.
     * @param exchange the http object encapsulating both the http request and response.
     * @param responseBody the response body used in the http response.
     * @throws IOException if port cannot get claimed
     */
    public static void httpContextCreator(HttpExchange exchange, Supplier<String> responseBody)
            throws IOException {
        final byte[] response = responseBody.get().getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
        exchange.getResponseBody().close();
    }

    /**
     * Shuts down the server and threadpool.
     */
    @AfterClass
    public static void shutdownServer() {
        logger.info(" Shutting down server started on port 28121");
        server.stop(0);
        executor.shutdownNow();
    }

    /**
     * Prepares for the test.
     *
     * @throws Exception if something goes wrong.
     */
    @Before
    public void prepareTest() throws Exception {
        IProbeContext probeContext = mock(IProbeContext.class);
        // set up probe related stuff
        IPropertyProvider propertyProvider = mock(IPropertyProvider.class);
        when(propertyProvider.getProperty(any())).thenReturn(30000);
        when(probeContext.getPropertyProvider()).thenReturn(propertyProvider);
        probePersistentData = mock(IProbeDataStoreEntry.class);
        when(probeContext.getTargetFeatureCategoryData(any())).thenReturn(probePersistentData);
        when(probePersistentData.load()).thenReturn(Optional.empty());
        probe = new WebhookProbe();
        probe.initialize(probeContext, null);
        progressTracker = mock(IProgressTracker.class);
        handler.resetHandler();
    }

    /**
     * Tests the case that workflow is a simple get call without body.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testSuccessfulGetMethod() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121/get/method")
                        .setHttpMethod(HttpMethodType.GET.name())
                        .build()))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.GET.name(), "/get/method", null,
                Collections.emptyList());
    }

    /**
     * Test request with no headers.
     *
     * @throws InterruptedException if interruption happens
     */
    @Test
    public void testSuccessfulRequestWithNoHeaders() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121/get/method")
                        .setHttpMethod(HttpMethodType.GET.name())
                        .build()))
                .build();

        // ACT
        final ActionResult result = probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.GET.name(), "/get/method", null,
                Collections.emptyList());
    }

    /**
     * Test request with multiple headers with the same name and different values.
     *
     * @throws InterruptedException if interruption happens
     */
    @Test
    public void testSuccessfulRequestWithMultipleHeadersWithTheSameName() throws InterruptedException {
        // ARRANGE
        final List<Pair<String, String>> requestHeaders = Arrays.asList(
                Pair.of("header_name", "header_value_1"),
                Pair.of("header_name", "header_value_2"));
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121/get/method")
                        .setHttpMethod(HttpMethodType.GET.name())
                        .setHeaders(requestHeaders)
                        .build()))
                .build();

        // ACT
        final ActionResult result = probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.GET.name(), "/get/method", null,
                requestHeaders);
    }

    /**
     * Test request with multiple different headers.
     *
     * @throws InterruptedException if interruption happens
     */
    @Test
    public void testSuccessfulRequestWithMultipleDifferentHeaders() throws InterruptedException {
        // ARRANGE
        final List<Pair<String, String>> requestHeaders = Arrays.asList(
                Pair.of("header_name_1", "header_value_1"),
                Pair.of("header_name_2", "header_value_2"));
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121/get/method")
                        .setHttpMethod(HttpMethodType.GET.name())
                        .setHeaders(requestHeaders)
                        .build()))
                .build();

        // ACT
        final ActionResult result = probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.GET.name(), "/get/method", null,
                requestHeaders);
    }

    /**
     * In a HTTP request, the only mandatory header is the 'Host' field.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testHttpServerReceivedRequiredHeaders() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121/get/method")
                        .setHttpMethod(HttpMethodType.GET.name())
                        .build()))
                .build();

        // ACT
        probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        final Optional<Map<String, List<String>>> headers = handler.getLastRequestHeaders();

        // ASSERT
        assertTrue(headers.isPresent());
        assertTrue(headers.get().containsKey(HttpHeaders.HOST));
    }

    /**
     * Tests that the http server did indeed receive the http headers being sent.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testHttpServerReceivedHeaders() throws InterruptedException {
        // ARRANGE
        final List<Pair<String, String>> requestHeaders = Arrays.asList(
                Pair.of("header_name_1", "header_value_1"),
                Pair.of("header_name_2", "header_value_2"));
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121/get/method")
                        .setHttpMethod(HttpMethodType.GET.name())
                        .setHeaders(requestHeaders)
                        .build()))
                .build();

        // ACT
        probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        final Optional<Map<String, List<String>>> headers = handler.getLastRequestHeaders();

        // ASSERT
        assertTrue(headers.isPresent());
        final Map<String, List<String>> headersMap = headers.get();
        assertTrue(headersMap.containsKey("header_name_1"));
        assertTrue(headersMap.containsKey("header_name_2"));
        assertEquals(headersMap.get("header_name_1").get(0), "header_value_1");
        assertEquals(headersMap.get("header_name_2").get(0), "header_value_2");
    }

    /**
     * Tests that an invalid endpoint returns a failed action result.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testInvalidEndpoint() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121/not/found")
                        .setHttpMethod(HttpMethodType.GET.name())
                        .build()))
                .build();

        // ACT
        final ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        assertEquals(ActionResponseState.FAILED, result.getState());
    }

    /**
     * Tests the case that workflow is a simple get call without body. The success code is 201 rather 200.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testSuccessfulGetMethod201() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121/get/method")
                        .setHttpMethod(HttpMethodType.GET.name())
                        .build()))
                .build();

        handler.addResponse(new Response(201, null));

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.GET.name(), "/get/method", null,
                Collections.emptyList());
    }

    /**
     * Tests the case that workflow is get call with  a templated body without body that results in failure.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testFailedUrlTemplatedGetMethod() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121/get/144151046183109")
                        .setHttpMethod(HttpMethodType.GET.name())
                        .build()))
                .build();

        handler.addResponse(new Response(404, "XYXYXY"));

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.FAILED, HttpMethodType.GET.name(), "/get/" + ACTION_UUID, null,
                Collections.emptyList());
        assertTrue(result.getDescription().contains("404"));
        assertTrue(result.getDescription().contains("XYXYXY"));
    }

    /**
     * Tests a post call with a fixed message to an endpoint.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testSuccessfulPostRequestWithBody() throws InterruptedException {
        // ARRANGE
        final String address = "/postEndpoint";
        final String payload = "{\"message\": \"sample message\"}";
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121" + address)
                        .setHttpMethod(HttpMethodType.POST.name())
                        .setTemplatedActionBody(payload)
                        .build()))
                .build();


        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.POST.name(), address, payload,
                Collections.emptyList());
    }

    /**
     * Tests a post call with a templated message to an endpoint.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testSuccessfulPostRequestWithATemplatedBody() throws InterruptedException {
        // ARRANGE
        final String address = "/";
        final String payload = "{\"id\": \"144151046183109\", \"type\": \"RESIZE\", \"commodity\":"
                + " \"$VCPU\", \"to\": \"3.0\"}";
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121" + address)
                        .setHttpMethod(HttpMethodType.POST.name())
                        .setTemplatedActionBody(payload)
                        .build()))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.POST.name(), address,
                payload, Collections.emptyList());
    }

    /**
     * Tests a post call with a templated message to an endpoint setting trustSelfSignedCertificate.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testSuccessfulPostRequestWithATemplatedBodyWithTrustSelfSignedCertificate() throws InterruptedException {
        // ARRANGE
        final String address = "/";
        final String payload = "{\"id\": \"144151046183109\", \"type\": \"RESIZE\", \"commodity\":"
                + " \"$VCPU\", \"to\": \"3.0\"}";
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121" + address)
                        .setHttpMethod(HttpMethodType.POST.name())
                        .setTemplatedActionBody(payload)
                        .setTrustSelfSignedCertificate("true")
                        .build()))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.POST.name(), address,
                payload, Collections.emptyList());
    }

    /**
     * Tests a put call with a templated message to an templated url endpoint.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testSuccessfulPutRequestWithATemplatedBody() throws InterruptedException {
        // ARRANGE
        final String address = "/actions/" + ACTION_UUID;
        final String payload = "{\"description\": \"Resize up VCPU for Virtual Machine turbonomic-t8c from 2 to 3\"}";
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121" + address)
                        .setHttpMethod(HttpMethodType.PUT.name())
                        .setTemplatedActionBody(payload)
                        .build()))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.PUT.name(), address,
                payload, Collections.emptyList());
    }

    /**
     * Tests the case that workflow is a get call with basic auth.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testBasicAuth() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121/get/method")
                        .setHttpMethod(HttpMethodType.GET.name())
                        .setAuthenticationMethod(AuthenticationMethod.BASIC.name())
                        .setUsername("testUser")
                        .setPassword("testPass")
                        .build()))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.GET.name(), "/get/method", null,
                Collections.emptyList());

        // make sure request has basic auth header
        assertThat(handler.getLastRequest().get().headers.get("Authorization"),
                equalTo(Collections.singletonList("Basic dGVzdFVzZXI6dGVzdFBhc3M=")));
    }

    /**
     * Test that when using oauth, an authorization header is sent in the form of
     * "Bearer {accessToken}".
     * @throws InterruptedException should not be thrown.
     */
    @Test
    public void testOAuth() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = oAuthAction();
        addSuccessfulOAuthResponses(ACCESS_TOKEN);

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        // make sure request has the authorization header which includes the bearer token
        Map<String, List<String>> headers = handler.getLastRequest().get().headers;
        assertNotNull(headers);
        assertTrue(headers.get(OAuthParameters.AUTHORIZATION.getParameterId()).get(0).contains("Bearer"));

        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.GET.name(), GET_METHOD_PATH,
                null, Collections.emptyList());
    }

    /**
     * Test that when requesting authorization to a server, a retry attempt occurs if the first
     * request fails (any 4xx failure).
     * We proceed by trying to request a new access token. If the access token is the same (it is in this case),
     * as the previous one new no request will be made and an exception will be thrown.
     * @throws Exception should not be thrown.
     */
    @Test
    public void testOAuthRetryAttemptWithSameToken() throws Exception {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = oAuthAction();
        // we already have the access token in the cache
        when(probePersistentData.load()).thenReturn(Optional.of(createSerializedMap(3600)));

        handler.responses.add(new Response(401, "Failed to Access"));
        handler.responses.add(new Response(200, String.format(OAUTH_RESPONSE,
                ACCESS_TOKEN)));

        // ACT
        final ActionResult actionResult = probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        List<Request> requests = handler.getRequests();
        assertNotNull(requests);
        assertEquals(2, requests.size());
        assertThat(actionResult.getState(), is(ActionResponseState.FAILED));
    }

    /**
     * Test that when requesting authorization to a server, a retry attempt occurs if the first
     * request fails (any 4xx failure). We proceed by trying to request a new access token and
     * attempting another attempt to request access to the authorizing server.
     * @throws Exception should not be thrown.
     */
    @Test
    public void testOAuthRetryAttemptWithDifferentTokens() throws Exception {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = oAuthAction();

        when(probePersistentData.load()).thenReturn(Optional.of(createSerializedMapWithRefreshToken()));

        handler.responses.add(new Response(401, "Failed to Access"));
        handler.responses.add(new Response(200, String.format(OAUTH_RESPONSE,
                SECOND_ACCESS_TOKEN)));

        // ACT
        final ActionResult actionResult = probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        //ASSERT
        assertEquals(handler.getRequests().size(), 3);
        // make the second call used refresh token
        final Request refreshToken = handler.getRequests().get(1);
        assertThat(Sets.newHashSet(refreshToken.payload.split("&")), is(Sets.newHashSet(
                "scope=test_scope", "grant_type=refresh_token",
                "refresh_token=refresh_tk1")));
        // make sure the main call was made
        verifyResults(actionResult, ActionResponseState.SUCCEEDED, HttpMethodType.GET.name(), GET_METHOD_PATH,
                null, Collections.emptyList());
    }

    /**
     * Tests the case where refresh token is used to get a new access token. However,
     * the refresh token is no longer valid. It is expected that we fall back to implied grant type.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testOAuthRefreshTokenFailure() throws Exception {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = oAuthAction();
        when(probePersistentData.load()).thenReturn(Optional.of(createSerializedMapWithRefreshToken()));

        handler.responses.add(new Response(401, "Failed to Access"));
        handler.responses.add(new Response(401,
                "{\"error\": \"invalid refresh token\""));
        handler.responses.add(new Response(200, String.format(OAUTH_RESPONSE,
                SECOND_ACCESS_TOKEN)));

        // ACT
        final ActionResult actionResult = probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        // ASSERT
        assertEquals(handler.getRequests().size(), 4);
        // make the second call used refresh token
        assertThat(Sets.newHashSet(handler.getRequests().get(1).payload.split("&")),
                is(Sets.newHashSet(
                "scope=test_scope", "grant_type=refresh_token",
                "refresh_token=refresh_tk1")));
        // make sure third call is not using refresh token
        assertThat(Sets.newHashSet(handler.getRequests().get(2).payload.split("&")),
                is(Sets.newHashSet(
                "scope=test_scope", "grant_type=client_credentials",
                "client_id=test_client_id", "client_secret=test_client_secret")));
        // make sure the main call was made
        verifyResults(actionResult, ActionResponseState.SUCCEEDED, HttpMethodType.GET.name(), GET_METHOD_PATH,
                null, Collections.emptyList());
    }

    /**
     * Test the case that we use probe data persistence to re-use access token.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testOauthDataPersistence() throws Exception {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = oAuthAction();
        addSuccessfulOAuthResponses(ACCESS_TOKEN);
        when(probePersistentData.compute(any())).thenAnswer(invocation -> {
            // verify serialization behaving as expected
            TokenManager.WebhookProbeDataStoreEntryUpdate update = invocation.getArgument(0);
            final Optional<byte[]> result = update.update(Optional.empty());
            Assert.assertTrue(result.isPresent());
            Map<AccessTokenKey, AccessTokenResponse> map = SerializationUtils
                    .deserialize(result.get());
            assertThat(map.size(), is(1));
            Map.Entry<AccessTokenKey, AccessTokenResponse> entry = map.entrySet().iterator().next();
            assertThat(entry.getKey(), is(new AccessTokenKey(OAUTH_URL, CLIENT_ID, CLIENT_SECRET,
                    GrantType.CLIENT_CREDENTIALS, SCOPE)));
            assertThat(entry.getValue().getAccessToken(), is(ACCESS_TOKEN));
            assertThat(entry.getValue().getTokenType(), is("Bearer"));
            assertThat(entry.getValue().getExpiresIn(), is(3599L));
            return null;
        });


        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        // ASSERT
        assertThat(handler.requests.size(), is(2));
        // verify actual call
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.GET.name(), GET_METHOD_PATH,
                null, Collections.emptyList());
        // verify oauth
        final Request oauthRequest = handler.requests.get(0);
        assertThat(oauthRequest.method, is(HttpMethodType.POST.name()));
        assertThat(oauthRequest.url, is("/oauth"));
        assertThat(Sets.newHashSet(oauthRequest.payload.split("&")), is(Sets.newHashSet(
                "scope=test_scope", "grant_type=client_credentials",
                "client_id=test_client_id", "client_secret=test_client_secret")));

        // verify the update is called
        verify(probePersistentData, times(1)).compute(any());
    }

    /**
     * Test the case when an oauth call is made when the access token already exists
     * in data persistence.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testOauthAccessTokenAlreadyExists() throws Exception {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = oAuthAction();
        // create the map
        when(probePersistentData.load()).thenReturn(Optional.of(createSerializedMap(3600)));

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        assertThat(handler.requests.size(), is(1));
        // verify actual call
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.GET.name(), GET_METHOD_PATH,
                null, Collections.emptyList());
        // verify the update is called
        verify(probePersistentData, times(0)).compute(any());
    }

    /**
     * Test the case when an oauth call is made when the access token already exists
     * in data persistence but it is expired.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testOauthAccessTokenAlreadyExistsExpired() throws Exception {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = oAuthAction();

        when(probePersistentData.compute(any())).thenAnswer(invocation -> {
            // verify serialization behaving as expected
            TokenManager.WebhookProbeDataStoreEntryUpdate update = invocation.getArgument(0);
            final Optional<byte[]> result = update.update(Optional.of(createSerializedMap(0)));
            Assert.assertTrue(result.isPresent());
            Map<AccessTokenKey, AccessTokenResponse> map = SerializationUtils
                    .deserialize(result.get());
            assertThat(map.size(), is(1));
            Map.Entry<AccessTokenKey, AccessTokenResponse> entry = map.entrySet().iterator().next();
            assertThat(entry.getKey(), is(new AccessTokenKey(OAUTH_URL, CLIENT_ID, CLIENT_SECRET,
                    GrantType.CLIENT_CREDENTIALS, SCOPE)));
            assertThat(entry.getValue().getAccessToken(), is(SECOND_ACCESS_TOKEN));
            assertThat(entry.getValue().getTokenType(), is("Bearer"));
            assertThat(entry.getValue().getExpiresIn(), is(3599L));
            return null;
        });

        // create the map
        when(probePersistentData.load()).thenReturn(Optional.of(createSerializedMap(0)));

        handler.responses.add(new Response(200, String.format(OAUTH_RESPONSE,
                SECOND_ACCESS_TOKEN)));
        handler.responses.add(new Response(200, "Test"));

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        assertThat(handler.requests.size(), is(2));
        // verify actual call
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.GET.name(), GET_METHOD_PATH,
                null, Collections.emptyList());
        // verify the update is called
        verify(probePersistentData, times(1)).compute(any());
    }

    /**
     * Test the case when an oauth call is made when the data for previous value is
     * corrupted.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testOauthAccessTokenPreviousValueDeserializationFailed() throws Exception {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = oAuthAction();

        when(probePersistentData.compute(any())).thenAnswer(invocation -> {
            // verify serialization behaving as expected
            TokenManager.WebhookProbeDataStoreEntryUpdate update = invocation.getArgument(0);
            final Optional<byte[]> result = update
                    .update(Optional.of(SerializationUtils.serialize(new ArrayList<>())));
            Assert.assertTrue(result.isPresent());
            Map<AccessTokenKey, AccessTokenResponse> map = SerializationUtils
                    .deserialize(result.get());
            assertThat(map.size(), is(1));
            Map.Entry<AccessTokenKey, AccessTokenResponse> entry = map.entrySet().iterator().next();
            assertThat(entry.getKey(), is(new AccessTokenKey(OAUTH_URL, CLIENT_ID, CLIENT_SECRET,
                    GrantType.CLIENT_CREDENTIALS, SCOPE)));
            assertThat(entry.getValue().getAccessToken(), is(ACCESS_TOKEN));
            assertThat(entry.getValue().getTokenType(), is("Bearer"));
            assertThat(entry.getValue().getExpiresIn(), is(3599L));
            return null;
        });

        // create the map
        when(probePersistentData.load()).thenReturn(
                Optional.of(SerializationUtils.serialize(new ConcurrentHashMap<>())));

        handler.responses.add(new Response(200, String.format(OAUTH_RESPONSE,
                ACCESS_TOKEN)));
        handler.responses.add(new Response(200, "Test"));

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account,
                Collections.emptyMap(), progressTracker);

        assertThat(handler.requests.size(), is(2));
        // verify actual call
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.GET.name(), GET_METHOD_PATH,
                null, Collections.emptyList());
        // verify the update is called
        verify(probePersistentData, times(1)).compute(any());
    }

    private byte[] createSerializedMap(long expireIn) {
        final ConcurrentHashMap<AccessTokenKey, AccessTokenResponse> map =
                new ConcurrentHashMap<>();
        final AccessTokenKey accessTokenKey = new AccessTokenKey(OAUTH_URL, CLIENT_ID, CLIENT_SECRET,
                GrantType.CLIENT_CREDENTIALS, SCOPE);
        final AccessTokenResponse accessTokenResponse = new AccessTokenResponse(ACCESS_TOKEN, "Bearer",
                expireIn, null, null, null, null);
        map.put(accessTokenKey, accessTokenResponse);
        return SerializationUtils.serialize(map);
    }

    private byte[] createSerializedMapWithRefreshToken() {
        // we already have the access token with refresh in the cache
        final ConcurrentHashMap<AccessTokenKey, AccessTokenResponse> map =
                new ConcurrentHashMap<>();
        final AccessTokenKey accessTokenKey = new AccessTokenKey(OAUTH_URL, CLIENT_ID, CLIENT_SECRET,
                GrantType.CLIENT_CREDENTIALS, SCOPE);
        final AccessTokenResponse accessTokenResponse = new AccessTokenResponse(ACCESS_TOKEN, "Bearer",
                3600, "refresh_tk1", null, null, null);
        map.put(accessTokenKey, accessTokenResponse);
        return SerializationUtils.serialize(map);
    }

    private ActionExecutionDTO oAuthAction() {
        return ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl(SERVER_URL + GET_METHOD_PATH)
                        .setHttpMethod(HttpMethodType.GET.name())
                        .setAuthenticationMethod(AuthenticationMethod.OAUTH.name())
                        .setOAuthUrl(OAUTH_URL)
                        .setClientId(CLIENT_ID)
                        .setClientSecret(CLIENT_SECRET)
                        .setScope(SCOPE)
                        .setGrantType(WebhookConstants.GrantType.CLIENT_CREDENTIALS.name())
                        .build()))
                .build();
    }

    private void addSuccessfulOAuthResponses(String accessToken) {
        handler.responses.add(new Response(200,
                String.format(OAUTH_RESPONSE, accessToken)));
        handler.responses.add(new Response(200, "Test"));
    }

    /**
     * Test sending newly generated actions to a webhook.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testSendingOnGenActionsToWebhook() throws InterruptedException {
        // ARRANGE
        final String address = "/actions/$action.uuid";
        final String payload = "{\"description\": \"$action.details\"}";
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121" + address)
                        .setHttpMethod(HttpMethodType.PUT.name())
                        .setTemplatedActionBody(payload)
                        .setAuthenticationMethod(AuthenticationMethod.NONE.name())
                        .build()))
                .build();
        final ActionEventDTO actionEventDTO = ActionEventDTO.newBuilder()
                .setAction(actionExecutionDTO)
                .setOldState(ActionResponseState.PENDING_ACCEPT)
                .setNewState(ActionResponseState.PENDING_ACCEPT)
                .setTimestamp(System.currentTimeMillis())
                .build();

        // ACT
        final Collection<ActionErrorDTO> actionErrorDTOS = probe.auditActions(account,
                Collections.singletonList(actionEventDTO));

        // ASSERT
        Assert.assertTrue(actionErrorDTOS.isEmpty());
    }

    /**
     * Tests audit actions to webhook.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testSendingOnGenActionsToWebhookFailure() throws InterruptedException {
        // ARRANGE
        final String address = "/actions/$action.uuid";
        final String payload = "{\"description\": \"$action.details\"}";
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121" + address)
                        .setHttpMethod(HttpMethodType.PUT.name())
                        .setTemplatedActionBody(payload)
                        .setAuthenticationMethod(AuthenticationMethod.NONE.name())
                        .build()))
                .build();
        final ActionEventDTO actionEventDTO = ActionEventDTO.newBuilder()
                .setAction(actionExecutionDTO)
                .setOldState(ActionResponseState.PENDING_ACCEPT)
                .setNewState(ActionResponseState.PENDING_ACCEPT)
                .setTimestamp(System.currentTimeMillis())
                .build();

        handler.addResponse(new Response(404, null));

        // ACT
        final Collection<ActionErrorDTO> actionErrorDTOS = probe.auditActions(account,
                Collections.singletonList(actionEventDTO));

        // ASSERT
        Assert.assertTrue(actionErrorDTOS.isEmpty());
    }

    /**
     * Test to verify the cleared actions are not audited.
     *
     * @throws InterruptedException if an error occurs..
     */
    @Test
    public void testClearedActionsAreNotAudited() throws InterruptedException {
        // ARRANGE
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setHttpMethod(HttpMethodType.PUT.name())
                        .setAuthenticationMethod(AuthenticationMethod.NONE.name())
                        .build()))
                .build();
        final ActionEventDTO actionEventDTO = ActionEventDTO.newBuilder()
                .setAction(actionExecutionDTO)
                .setOldState(ActionResponseState.PENDING_ACCEPT)
                .setNewState(ActionResponseState.CLEARED)
                .setTimestamp(System.currentTimeMillis())
                .build();

        // ACT
        final Collection<ActionErrorDTO> actionErrorDTOS = probe.auditActions(account,
                Collections.singletonList(actionEventDTO));

        // ASSERT
        Assert.assertTrue(actionErrorDTOS.isEmpty());
    }

    /**
     * Tests that the payload is not templated when 'hasTemplateApplied' is set to true.
     *
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testTemplatingNotApplied() throws InterruptedException {
        // ARRANGE
        final String address = "/";
        final String payload = "{\"id\": \"$action.uuid\", \"type\": \"$action.actionType\", \"commodity\":"
                + " \"$action.risk.reasonCommodities.toArray()[0]\", \"to\": \"$action.newValue\"}";
        final ActionExecutionDTO actionExecutionDTO = ON_PREM_RESIZE_ACTION.toBuilder().setWorkflow(
                createWorkflow(new WebhookPropertiesBuilder()
                        .setUrl("http://localhost:28121" + address)
                        .setHttpMethod(HttpMethodType.GET.name())
                        .setTemplatedActionBody(payload)
                        .build()))
                .build();

        // ACT
        ActionResult result = probe.executeAction(actionExecutionDTO, account, Collections.emptyMap(), progressTracker);

        // ASSERT
        verifyResults(result, ActionResponseState.SUCCEEDED, HttpMethodType.GET.name(), address,
                payload, Collections.emptyList());
    }

    private void verifyResults(ActionResult result, ActionResponseState expectedState, String expectedMethod,
            String expectedAddress, String expectedPayload, final List<Pair<String, String>> headers) {
        assertThat(result.getState(), is(expectedState));
        Optional<Request> request = handler.getLastRequest();
        assertTrue(request.isPresent());
        assertThat(request.get().method, is(expectedMethod));
        assertThat(request.get().url, is(expectedAddress));
        assertThat(request.get().payload, is(expectedPayload));
        // verify headers if any
        if (!headers.isEmpty()) {
            final Map<String, List<String>> sentHeaders = convertListOfHeadersIntoMap(headers);
            final Map<String, List<String>> receivedHeaders = request.get().headers;
            sentHeaders.forEach((key, value) -> {
                final List<String> headerValues = receivedHeaders.get(key);
                assertEquals(headerValues, value);
            });
        }
    }

    private Map<String, List<String>> convertListOfHeadersIntoMap(
            final List<Pair<String, String>> headers) {
        final Map<String, List<String>> sentHeaders = new HashMap<>();
        for (Pair<String, String> requestHeader : headers) {
            sentHeaders.computeIfAbsent(requestHeader.getKey(), k -> new ArrayList<>()).add(
                    requestHeader.getValue());
        }
        return sentHeaders;
    }

    /**
     * A response handler that caches the last request made to it.
     */
    private static class CachingHttpHandler implements HttpHandler {
        private final List<Request> requests = new ArrayList<>();
        private final Queue<Response> responses = new LinkedList<>();


        @Override
        public void handle(HttpExchange exchange) throws IOException {
            final String body;
            if (exchange.getRequestBody().available() != 0) {
                body = IOUtils.toString(exchange.getRequestBody(), StandardCharsets.UTF_8.name());
            } else {
                body = null;
            }

            requests.add(new Request(exchange.getRequestMethod(),
                    exchange.getRequestURI().toString(), body,
                    exchange.getRequestHeaders()));

            if (!responses.isEmpty()) {
                final Response response = responses.remove();
                final byte[] responseBytes = response.responseBody != null
                        ? response.responseBody.getBytes(StandardCharsets.UTF_8) : new byte[0];
                exchange.sendResponseHeaders(response.responseCode, responseBytes.length);
                exchange.getResponseBody().write(responseBytes);
            } else {
                exchange.sendResponseHeaders(200, 0);
            }
            exchange.getResponseBody().close();
        }

        void addResponse(Response response) {
            this.responses.add(response);
        }

        Optional<Request> getLastRequest() {
            return requests.isEmpty() ? Optional.empty()
                    : Optional.of(requests.get(requests.size() - 1));
        }

        List<Request> getRequests() {
            return requests;
        }

        Optional<Map<String, List<String>>> getLastRequestHeaders() {
            return getLastRequest().flatMap(Request::getHeaders);
        }

        void resetHandler() {
            requests.clear();
            responses.clear();
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

        Optional<Map<String, List<String>>> getHeaders() {
            return Optional.ofNullable(headers);
        }
    }

    /**
     * Keeps the information about a response.
     */
    private static class Response {
        private final int responseCode;
        private final String responseBody;

        Response(int responseCode, String responseBody) {
            this.responseCode = responseCode;
            this.responseBody = responseBody;
        }
    }
}
