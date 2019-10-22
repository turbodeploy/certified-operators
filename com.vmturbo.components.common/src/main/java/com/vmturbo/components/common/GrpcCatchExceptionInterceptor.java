package com.vmturbo.components.common;

import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Interceptor is intended to pretty print the exception throws from the gRPC service.
 * This interceptor MUST be the last in the interceptors list, as it does not propagate
 * exceptions further.
 */
public class GrpcCatchExceptionInterceptor implements ServerInterceptor {

    private final Logger logger = LogManager.getLogger(getClass());

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
            Metadata requestHeaders, ServerCallHandler<ReqT, RespT> next) {
        ServerCall.Listener<ReqT> delegate = next.startCall(call, requestHeaders);
        return new SimpleForwardingServerCallListener<ReqT>(delegate) {
            @Override
            public void onHalfClose() {
                try {
                    super.onHalfClose();
                } catch (RuntimeException e) {
                    logger.error(
                            "gRPC service call " + call.getMethodDescriptor().getFullMethodName() +
                                    " failed", e);
                    call.close(Status.INTERNAL.withCause(e).withDescription("error message"),
                            new Metadata());
                }
            }
        };
    }
}
