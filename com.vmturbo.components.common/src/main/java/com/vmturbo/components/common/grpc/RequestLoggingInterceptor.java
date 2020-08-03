package com.vmturbo.components.common.grpc;

import java.util.concurrent.atomic.AtomicLong;

import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * gRPC interceptor intended to log all the gRPC requests timings into log4j framework.
 */
public class RequestLoggingInterceptor implements ServerInterceptor {
    private final Logger logger = LogManager.getLogger(getClass());

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
            Metadata requestHeaders, ServerCallHandler<ReqT, RespT> next) {
        final long time = System.currentTimeMillis();
        final AtomicLong requestFinishedTime = new AtomicLong(time);
        final ServerCall.Listener<ReqT> delegate = next.startCall(call, requestHeaders);
        return new SimpleForwardingServerCallListener<ReqT>(delegate) {

            @Override
            public void onHalfClose() {
                final long execTime = System.currentTimeMillis() - time;
                logger.debug("gRPC call {}.{} finished receiving request in {}ms",
                        call.getMethodDescriptor().getServiceName(),
                        call.getMethodDescriptor().getFullMethodName(), execTime);
                requestFinishedTime.set(System.currentTimeMillis());
                super.onHalfClose();
            }

            @Override
            public void onComplete() {
                final long execTime = System.currentTimeMillis() - requestFinishedTime.get();
                logger.debug("gRPC call {}.{} executed in {}ms",
                        call.getMethodDescriptor().getServiceName(),
                        call.getMethodDescriptor().getFullMethodName(), execTime);
                super.onComplete();
            }
        };
    }
}
