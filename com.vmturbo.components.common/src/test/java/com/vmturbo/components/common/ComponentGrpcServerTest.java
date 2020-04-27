package com.vmturbo.components.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.net.BindException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status.Code;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.util.SocketUtils;

import com.vmturbo.common.protobuf.logging.LogConfigurationServiceGrpc;
import com.vmturbo.common.protobuf.logging.LogConfigurationServiceGrpc.LogConfigurationServiceBlockingStub;
import com.vmturbo.common.protobuf.logging.Logging.GetLogLevelsRequest;
import com.vmturbo.common.protobuf.logging.Logging.GetLogLevelsResponse;
import com.vmturbo.common.protobuf.logging.Logging.LogLevel;
import com.vmturbo.common.protobuf.logging.LoggingMoles.LogConfigurationServiceMole;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;

/**
 * Unit tests for {@link ComponentGrpcServer}.
 */
public class ComponentGrpcServerTest {

    private LogConfigurationServiceMole logConfigurationServiceMole = spy(LogConfigurationServiceMole.class);

    /**
     * Allows overrides of environment properties in tests.
     */
    @Rule
    public EnvironmentVariables environmentVariables = new EnvironmentVariables();

    /**
     * Allows easy capturing and verification of expected exceptions.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @After
    public void teardown() {
        ComponentGrpcServer.get().stop();
    }

    /**
     * Test the basic lifecycle of the {@link ComponentGrpcServer} - start and stop!
     */
    @Test
    public void testStartStop() {

        // Find a random port to use.
        final MockEnvironment env = new MockEnvironment();

        final TestInterceptor interceptor = new TestInterceptor();

        ComponentGrpcServer.get().addServices(
                Collections.singletonList(logConfigurationServiceMole),
                Collections.singletonList(interceptor));

        // Start the server.
        startGrpcServer(env);

        // Set up the mock response.
        ComponentGrpcServer server = ComponentGrpcServer.get();
        final LogConfigurationServiceBlockingStub client =
            LogConfigurationServiceGrpc.newBlockingStub(ComponentGrpcServer.newChannelBuilder("localhost", server.getPort()).build());
        GetLogLevelsResponse logLevelsResponse = GetLogLevelsResponse.newBuilder()
            .putLogLevels("foo", LogLevel.INFO)
            .build();
        when(logConfigurationServiceMole.getLogLevels(any()))
            .thenReturn(logLevelsResponse);

        // So far there have been no RPC calls, so the interceptor shouldn't have intercepted
        // anything.
        assertThat(interceptor.callClosed.get(), is(false));

        // Check that an RPC to the server works.
        assertThat(client.getLogLevels(GetLogLevelsRequest.getDefaultInstance()), is(logLevelsResponse));

        // Check that the interceptor got called.
        assertThat(interceptor.callClosed.get(), is(true));

        // Stop the server.
        ComponentGrpcServer.get().stop();

        // The server should have shut down.
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.UNAVAILABLE).anyDescription());
        client.getLogLevels(GetLogLevelsRequest.getDefaultInstance());
    }

    /**
     * Test that adding services to the {@link ComponentGrpcServer} after it's already been started
     * works - the internal gRPC server should restart transparently, and the new services should
     * be registered.
     */
    @Test
    public void testAddServicesAfterStartRestartsServer() {
        // Find a random port to use.
        final MockEnvironment env = new MockEnvironment();

        ComponentGrpcServer.get().addServices(
            Collections.singletonList(logConfigurationServiceMole),
            Collections.emptyList());

        // Start the server.
        startGrpcServer(env);

        // Create a client.
        ComponentGrpcServer server = ComponentGrpcServer.get();
        final LogConfigurationServiceBlockingStub client =
            LogConfigurationServiceGrpc.newBlockingStub(ComponentGrpcServer.newChannelBuilder("localhost", server.getPort()).build());


        LogConfigurationServiceMole updatedMole = spy(LogConfigurationServiceMole.class);
        // Only the UPDATED mole returns the expected result. The original mole will return
        // empty by default.
        GetLogLevelsResponse logLevelsResponse = GetLogLevelsResponse.newBuilder()
            .putLogLevels("foo", LogLevel.INFO)
            .build();
        when(updatedMole.getLogLevels(any()))
            .thenReturn(logLevelsResponse);

        // This should trigger a server restart so the updated mole can be registered.
        // The restart should be transparent to the client, so we shouldn't have to create a
        // new stub.
        ComponentGrpcServer.get().addServices(
                Collections.singletonList(updatedMole),
                Collections.emptyList());

        // Check that an RPC to the server works.
        assertThat(client.getLogLevels(GetLogLevelsRequest.getDefaultInstance()), is(logLevelsResponse));
    }

    /*
     * Start the gRPC server, retrying a few times if we lose a port allocation race.
     *
     * <p>Tests in this class have failed and casued commits to be revereted, because they lose
     * an unavoidable race condition that appears when attempting to bind an available port for
     * use by our gRPC server. The only way to do this is to find an unused port and then try to
     * bind it. Between those steps, any other process on the system could do the same and bind
     * the port, causing our attempt to bind to fail. This is a rare failure, and retrying a few
     * times should eliminate resulting build failures.</p>
     *
     * @param env mock environment to hold desired port number
     */
    private void startGrpcServer(final MockEnvironment env) {
        for (int tries = 1; ; tries++) {
            try {
                env.setProperty(ComponentGrpcServer.PROP_SERVER_GRPC_PORT,
                        Integer.toString(SocketUtils.findAvailableTcpPort()));
                ComponentGrpcServer.get().start(env);
                // succeeded!
                return;
            } catch (Exception e) {
                Throwable cause = e;
                // try up to three times, as long as we're catching BindException
                while (cause != null && !(cause instanceof BindException)) {
                    cause = cause.getCause();
                }
                if (cause == null || tries >= 3) {
                    // we caught something that wasn't ulimately a BindException, or we ran
                    // out of tries, so re-throw what we last caught
                    throw e;
                }
            }
        }
    }

    /**
     * gRPC interceptor to test that the {@link ComponentGrpcServer} registers interceptors
     * properly.
     */
    private class TestInterceptor implements ServerInterceptor {

        private final AtomicBoolean callClosed = new AtomicBoolean(false);

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                     Metadata requestHeaders, ServerCallHandler<ReqT, RespT> next) {
            ServerCall.Listener<ReqT> delegate = next.startCall(call, requestHeaders);
            return new SimpleForwardingServerCallListener<ReqT>(delegate) {
                @Override
                public void onHalfClose() {
                    callClosed.set(true);
                    super.onHalfClose();
                }
            };
        }
    }
}
