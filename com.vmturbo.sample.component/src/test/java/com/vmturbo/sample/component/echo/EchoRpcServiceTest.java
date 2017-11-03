package com.vmturbo.sample.component.echo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.sample.Echo.EchoRequest;
import com.vmturbo.common.protobuf.sample.Echo.EchoResponse;
import com.vmturbo.common.protobuf.sample.Echo.MultiEchoRequest;
import com.vmturbo.common.protobuf.sample.EchoServiceGrpc;
import com.vmturbo.common.protobuf.sample.EchoServiceGrpc.EchoServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.sample.component.notifications.SampleComponentNotificationSender;

/**
 * These are the unit tests for the gRPC service.
 *
 * You may also find the testing section in the following page useful:
 *     https://vmturbo.atlassian.net/wiki/display/Home/Using+gRPC
 */
public class EchoRpcServiceTest {

    /**
     * How long to wait for the echo service to issue a notification
     * that the echo was sent.
     */
    private static final long NOTIFICATION_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);

    private SampleComponentNotificationSender notificationsBackend =
            Mockito.mock(SampleComponentNotificationSender.class);

    private EchoRpcService echoRpcServiceBackend =
            new EchoRpcService(notificationsBackend, 0);

    /**
     * When testing gRPC services the best way to go is to test using a gRPC client-side stub.
     * That way your testing is going through the same code paths that remote calls will go
     * through.
     */
    private EchoServiceBlockingStub echoRpcServiceClient;

    /**
     * The GrpcTestServer is our wrapper class around gRPC's server.
     * It provides an easy way to initialize a server for testing and get
     * a channel to it, without worrying about the details.
     */
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(echoRpcServiceBackend);

    private static final String ECHO = "test";

    @Before
    public void startup() throws IOException {
        echoRpcServiceClient = EchoServiceGrpc.newBlockingStub(server.getChannel());
    }

    @Test
    public void testSingleEcho() {
        final EchoResponse response = echoRpcServiceClient.singleEcho(EchoRequest.newBuilder()
            .setEcho(ECHO)
            .build());

        Assert.assertEquals(ECHO, response.getEcho());
    }

    @Test
    public void testMultiEcho() {
        final int times = 2;
        Iterable<EchoResponse> responseIt = () -> echoRpcServiceClient.multiEcho(
                MultiEchoRequest.newBuilder()
                    .setRequest(EchoRequest.newBuilder()
                        .setEcho(ECHO))
                    .setTimes(times)
                    .build());
        List<EchoResponse> allResponses = StreamSupport.stream(responseIt.spliterator(), false)
                .collect(Collectors.toList());

        Assert.assertEquals(times, allResponses.size());
        allResponses.forEach(response -> Assert.assertEquals(ECHO, response.getEcho()));
    }

    @Test
    public void testSingleEchoNotification() throws Exception {
        echoRpcServiceClient.singleEcho(EchoRequest.newBuilder()
                .setEcho(ECHO)
                .build());

        Mockito.verify(notificationsBackend, Mockito.timeout(NOTIFICATION_TIMEOUT_MILLIS))
               .notifyEchoResponse(Mockito.any());
    }

    @Test
    public void testMultiEchoNotification() throws Exception {
        final int times = 2;
        echoRpcServiceClient.multiEcho(MultiEchoRequest.newBuilder()
            .setRequest(EchoRequest.newBuilder()
                .setEcho(ECHO))
            .setTimes(times)
            .build());

        Mockito.verify(notificationsBackend, Mockito.timeout(NOTIFICATION_TIMEOUT_MILLIS))
                .notifyEchoResponse(Mockito.any());
    }
}
