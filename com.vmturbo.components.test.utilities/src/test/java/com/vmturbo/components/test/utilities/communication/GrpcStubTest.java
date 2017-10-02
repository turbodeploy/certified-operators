package com.vmturbo.components.test.utilities.communication;

import org.junit.Assert;
import org.junit.Test;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceImplBase;
import com.vmturbo.common.protobuf.sample.Echo.EchoRequest;
import com.vmturbo.common.protobuf.sample.Echo.EchoResponse;
import com.vmturbo.common.protobuf.sample.EchoServiceGrpc;
import com.vmturbo.common.protobuf.sample.EchoServiceGrpc.EchoServiceBlockingStub;
import com.vmturbo.common.protobuf.sample.EchoServiceGrpc.EchoServiceImplBase;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.grpc.extensions.PingingChannelBuilder;

public class GrpcStubTest {

    @Test
    public void testGrpcStub() {
        final StubEchoService echoService = new StubEchoService();

        try (ComponentStubHost server = ComponentStubHost.newBuilder()
                .withGrpcServices(echoService)
                .build()) {
            server.start();
            Channel channel = PingingChannelBuilder
                    .forAddress("localhost", ComponentUtils.GLOBAL_GRPC_PORT)
                    .usePlaintext(true)
                    .build();
            EchoServiceBlockingStub stub = EchoServiceGrpc.newBlockingStub(channel);
            EchoResponse resp = stub.singleEcho(EchoRequest.newBuilder()
                    .setEcho("test")
                    .build());
            Assert.assertEquals("test", resp.getEcho());
        }
    }

    @Test
    public void testTwoGrpcStubs() {
        final StubEchoService echoService = new StubEchoService();
        final StubActionsService actionsService = new StubActionsService();

        try (ComponentStubHost server = ComponentStubHost.newBuilder()
                .withGrpcServices(echoService, actionsService)
                .build()) {
            server.start();
            final Channel channel = PingingChannelBuilder
                    .forAddress("localhost", ComponentUtils.GLOBAL_GRPC_PORT)
                    .usePlaintext(true)
                    .build();
            final EchoServiceBlockingStub stub = EchoServiceGrpc.newBlockingStub(channel);
            final EchoResponse resp = stub.singleEcho(EchoRequest.newBuilder()
                    .setEcho("test")
                    .build());
            Assert.assertEquals("test", resp.getEcho());

            final ActionsServiceBlockingStub actionStub =
                    ActionsServiceGrpc.newBlockingStub(channel);
            final AcceptActionResponse actionResp =
                    actionStub.acceptAction(SingleActionRequest.getDefaultInstance());
            Assert.assertEquals("test", actionResp.getError());
        }
    }

    private static class StubEchoService extends EchoServiceImplBase {

        public void singleEcho(EchoRequest request,
                               StreamObserver<EchoResponse> responseObserver) {
            final EchoResponse response = EchoResponse.newBuilder()
                    .setEcho(request.getEcho())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    private static class StubActionsService extends ActionsServiceImplBase {
        public void acceptAction(SingleActionRequest request,
                                 StreamObserver<AcceptActionResponse> responseObserver) {
            responseObserver.onNext(AcceptActionResponse.newBuilder().setError("test").build());
            responseObserver.onCompleted();
        }
    }
}
