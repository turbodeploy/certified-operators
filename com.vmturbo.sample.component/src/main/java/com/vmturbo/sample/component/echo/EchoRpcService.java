package com.vmturbo.sample.component.echo;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.sample.Echo.EchoRequest;
import com.vmturbo.common.protobuf.sample.Echo.EchoResponse;
import com.vmturbo.common.protobuf.sample.Echo.MultiEchoRequest;
import com.vmturbo.common.protobuf.sample.EchoServiceGrpc.EchoServiceImplBase;
import com.vmturbo.sample.component.notifications.SampleComponentNotificationSender;

/**
 * This is the implementation of the gRPC EchoService service defined in
 * com.vmturbo.common.protobuf/src/main/protobuf/sample/Echo.proto.
 *
 * See: http://www.grpc.io/docs/tutorials/basic/java.html for more info.
 */
public class EchoRpcService extends EchoServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final SampleComponentNotificationSender notificationsBackend;

    private final ScheduledExecutorService echoExecutor =
            Executors.newSingleThreadScheduledExecutor();

    /**
     * Constructor for the {@link EchoRpcService}. This should be called from a
     * \@Configuration-annotated * class to create an {@link EchoRpcService} bean.
     *
     * @param notificationsBackend The backend for sending notifications to websocket
     *                             listeners (i.e. other components).
     * @param randomIntProperty A random int property. It's here to illustrate that the standard
     *                          is to pass properties via the constructor from the configuration,
     *                          instead of using @Value annotations directly in the class.
     */
    public EchoRpcService(final SampleComponentNotificationSender notificationsBackend,
                          final int randomIntProperty) {
        this.notificationsBackend = Objects.requireNonNull(notificationsBackend);
        logger.info("Random int property: {}", randomIntProperty);
    }

    public void singleEcho(EchoRequest request,
                           StreamObserver<EchoResponse> responseObserver) {
        final EchoResponse response = EchoResponse.newBuilder()
                .setEcho(request.getEcho())
                .build();
        // In the singleEcho case there is only one response.
        responseObserver.onNext(response);
        // Every StreamObserver must have either an onCompleted() or an
        // onError() call to terminate the RPC.
        responseObserver.onCompleted();
        // Send notifications to all (remote) listeners about the echo response.
        notificationsBackend.notifyEchoResponse(response);
    }

    public void multiEcho(MultiEchoRequest request,
                          StreamObserver<EchoResponse> responseObserver) {
        final EchoResponse response = EchoResponse.newBuilder()
                .setEcho(request.getRequest().getEcho())
                .build();
        // In the multiEcho case there may be multiple responses.
        for (int i = 0; i < request.getTimes(); ++i) {
            responseObserver.onNext(response);
        }
        // Every StreamObserver must have either an onCompleted() or an
        // onError() call to terminate the RPC.
        responseObserver.onCompleted();
        // Send notifications to all (remote) listeners about the echo response.
        notificationsBackend.notifyEchoResponse(response);
    }
}
