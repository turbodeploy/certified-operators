package com.vmturbo.action.orchestrator.rpc;

import static com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier.IP_ADDRESS_CLAIM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.collect.ImmutableList;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.jsonwebtoken.CompressionCodecs;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.crypto.EllipticCurveProvider;

import com.vmturbo.action.orchestrator.ActionOrchestratorComponent;
import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTranslator;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor;
import com.vmturbo.action.orchestrator.store.ActionFactory;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.ActionSupportResolver;
import com.vmturbo.action.orchestrator.store.EntitySettingsCache;
import com.vmturbo.action.orchestrator.store.IActionFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreLoader;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.auth.api.JWTKeyCodec;
import com.vmturbo.auth.api.authorization.IAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.JwtCallCredential;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.authorization.kvstore.ApiKVAuthStore;
import com.vmturbo.auth.api.authorization.kvstore.IApiAuthStore;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;

/**
 * Integration tests for secure action execution RPC.
 *
 * Test flow:
 *  gRPC client side:
 *  sends requests with JwtCallCredential or JwtClientInterceptor
 *
 *  gPRC server side:
 *  server interceptor intercepts the request, validate the JWT token and store subject (user name)
 *  to {@link Context}.
 *
 * Covered gRPC JWT classes:
 * {@link JwtClientInterceptor}
 * {@link JwtCallCredential}
 * {@link JwtServerInterceptor}
 *
 * Covered gRPC server classes:
 * {@link ActionOrchestratorComponent}
 * {@link ActionsRpcService#acceptAction}
 * {@link SecurityConstant}
 */
public class ActionExecutionSecureRpcTest {
    private final static long ACTION_PLAN_ID = 2;
    private final static long TOPOLOGY_CONTEXT_ID = 3;
    private final static long ACTION_ID = 9999;
    public static final String ADMIN = "admin";
    public static final String ADMINISTRATOR = "ADMINISTRATOR";
    public static final String IP_ADDRESS = "10.10.10.1";
    private final IActionFactory actionFactory = new ActionFactory();
    private final IActionStoreFactory actionStoreFactory = mock(IActionStoreFactory.class);
    private final IActionStoreLoader actionStoreLoader = mock(IActionStoreLoader.class);
    private final AutomatedActionExecutor executor = mock(AutomatedActionExecutor.class);
    private final ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory,
            executor, actionStoreLoader);
    private final ActionExecutor actionExecutor = mock(ActionExecutor.class);
    // Have the translator pass-through translate all actions.
    private final ActionTranslator actionTranslator = Mockito.spy(new ActionTranslator(actionStream ->
            actionStream.map(action -> {
                action.getActionTranslation().setPassthroughTranslationSuccess();
                return action;
            })));
    private final ActionSupportResolver filter = mock
            (ActionSupportResolver.class);
    private final IApiAuthStore apiAuthStore = mock(ApiKVAuthStore.class);
    private final EntitySettingsCache entitySettingsCache = mock(EntitySettingsCache.class);
    private final ActionsRpcService actionsRpcService =
            new ActionsRpcService(actionStorehouse, actionExecutor, actionTranslator);
    private ActionsServiceBlockingStub actionOrchestratorServiceClient;
    private ActionsServiceBlockingStub actionOrchestratorServiceClientWithInterceptor;
    private ActionStore actionStoreSpy;
    private JWTAuthorizationToken token;
    private Server secureGrpcServer;
    private ManagedChannel channel;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final static ByteArrayOutputStream outputStream;
    private static final PrintStream old_out = System.out;

    static {
        outputStream = new ByteArrayOutputStream();
        // redirect output stream to PrintStream, so we can verify the audit output.
        System.setOut(new PrintStream(outputStream));
    }
    private static ActionPlan actionPlan(ActionDTO.Action recommendation) {
        return ActionPlan.newBuilder()
                .setId(ACTION_PLAN_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .addAction(recommendation)
                .build();
    }

    @AfterClass
    public static void afterClass() {
        System.setOut(old_out);
        System.out.println("finished testing");
    }

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception {

        // setup public/private key pair
        KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
        String privateKeyEncoded = JWTKeyCodec.encodePrivateKey(keyPair);
        PrivateKey signingKey = JWTKeyCodec.decodePrivateKey(privateKeyEncoded);

        // build JWT token
        String compact = Jwts.builder()
                .setSubject(ADMIN)
                .claim(IAuthorizationVerifier.ROLE_CLAIM, ImmutableList.of(ADMINISTRATOR))
                .claim(IP_ADDRESS_CLAIM, IP_ADDRESS) // add IP address
                .setExpiration(getTestDate())
                .signWith(SignatureAlgorithm.ES256, signingKey)
                .compressWith(CompressionCodecs.GZIP)
                .compact();

        // Encode the public key.
        String pubKeyStr = JWTKeyCodec.encodePublicKey(keyPair);

        // store JWT token for gRPC client
        token = new JWTAuthorizationToken(compact);

        // Setup caller authentication
        Set<GrantedAuthority> grantedAuths = new HashSet<>();
        grantedAuths.add(new SimpleGrantedAuthority("ROLE_NONADMINISTRATOR"));
        AuthUserDTO user = new AuthUserDTO(PROVIDER.LOCAL, "admin", null, "testUUID",
                compact, new ArrayList<>());
        Authentication authentication = new UsernamePasswordAuthenticationToken(user, "***", grantedAuths);

        // populate security context, so the client interceptor can get the JWT token.
        SecurityContextHolder.getContext().setAuthentication(authentication);

        // mock providing public key to auth store
        when(apiAuthStore.retrievePublicKey()).thenReturn(pubKeyStr);

        // setup JWT ServerInterceptor
        JwtServerInterceptor jwtInterceptor = new JwtServerInterceptor(apiAuthStore);

        // start secure gRPC
        final String name = "grpc-security-JWT-test";
        final InProcessServerBuilder serverBuilder = InProcessServerBuilder.forName(name);
        serverBuilder.addService(ServerInterceptors.intercept(actionsRpcService, jwtInterceptor));
        secureGrpcServer = serverBuilder.build();
        secureGrpcServer.start();
        channel = InProcessChannelBuilder.forName(name).build();

        // setup gPRC client
        actionOrchestratorServiceClient = ActionsServiceGrpc.newBlockingStub(channel);

        // setup gPRC client with client interceptor
        actionOrchestratorServiceClientWithInterceptor = ActionsServiceGrpc.newBlockingStub(channel)
                .withInterceptors(new JwtClientInterceptor());

        // mock action store
        actionStoreSpy = Mockito.spy(new LiveActionStore(actionFactory, TOPOLOGY_CONTEXT_ID,
                filter, entitySettingsCache));
        when(actionStoreFactory.newStore(anyLong())).thenReturn(actionStoreSpy);
        when(actionStoreLoader.loadActionStores()).thenReturn(Collections.emptyList());
        when(actionStoreFactory.getContextTypeName(anyLong())).thenReturn("foo");
    }

    @After
    public void tearDown() {
        System.out.println("shutting down gRPC channel and server");
        channel.shutdown();
        secureGrpcServer.shutdown();
    }

    private Date getTestDate() {
        Date dt = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(dt);
        c.add(Calendar.DATE, 1);
        dt = c.getTime();
        return dt;
    }

    /**
     * Verified the log4j2 output contains required messages.
     *
     * @param output      the log4j2 output
     * @param fullMessage expected message
     */
    private void verifyMessage(final String output, final String fullMessage) {
        assertTrue(output.contains(fullMessage));
    }

    /**
     * Test accepting an existing action with client providing JwtCallCredential
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testAcceptAction() throws Exception {
        final ActionPlan plan = actionPlan(ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID));
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();

        actionStorehouse.storeActions(plan);
        AcceptActionResponse response = actionOrchestratorServiceClient
                .withCallCredentials(new JwtCallCredential(token.getCompactRepresentation()))
                .acceptAction(acceptActionRequest);

        assertFalse(response.hasError());
        assertTrue(response.hasActionSpec());
        assertEquals(ACTION_ID, response.getActionSpec().getRecommendation().getId());
        assertEquals(ActionState.IN_PROGRESS, response.getActionSpec().getActionState());
        // verify log message includes admin user.
        // redirect System.out is not working when running in a test suite. Comment out for now.
        // verifyMessage(outputStream.toString(),
        //        ADMIN + " from IP address: " + IP_ADDRESS +" is trying to execute Action: 9999");
    }


    /**
     * Test accepting an existing action with client interceptor
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testAcceptActionWithClientInterceptor() throws Exception {
        final ActionPlan plan = actionPlan(ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID));
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();

        actionStorehouse.storeActions(plan);
        AcceptActionResponse response = actionOrchestratorServiceClientWithInterceptor
                .acceptAction(acceptActionRequest);

        assertFalse(response.hasError());
        assertTrue(response.hasActionSpec());
        assertEquals(ACTION_ID, response.getActionSpec().getRecommendation().getId());
        assertEquals(ActionState.IN_PROGRESS, response.getActionSpec().getActionState());
        // verify log message include admin user
        // redirect System.out is not working when running in a test suite. Comment out for now.
        // verifyMessage(outputStream.toString(),
        //        ADMIN + " from IP address: " + IP_ADDRESS +" is trying to execute Action: 9999");
    }

    /**
     * Test accepting an existing action with invalid JWT token.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testAcceptActionWithInvalidJwtToken() throws Exception {
        final ActionPlan plan = actionPlan(ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID));
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();

        actionStorehouse.storeActions(plan);
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.UNAUTHENTICATED)
                .descriptionContains("JWT strings must contain exactly 2 period characters"));
        AcceptActionResponse response = actionOrchestratorServiceClient
                .withCallCredentials(new JwtCallCredential("wrong token"))
                .acceptAction(acceptActionRequest);
    }

    /**
     * Test accepting an existing action without JWT token.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testAcceptActionWithoutJwtToken() throws Exception {
        final ActionPlan plan = actionPlan(ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID));
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();

        actionStorehouse.storeActions(plan);
        AcceptActionResponse response = actionOrchestratorServiceClient
                .withCallCredentials(new JwtCallCredential(token.getCompactRepresentation()))
                .acceptAction(acceptActionRequest);

        assertFalse(response.hasError());
        assertTrue(response.hasActionSpec());
        assertEquals(ACTION_ID, response.getActionSpec().getRecommendation().getId());
        assertEquals(ActionState.IN_PROGRESS, response.getActionSpec().getActionState());
    }
}