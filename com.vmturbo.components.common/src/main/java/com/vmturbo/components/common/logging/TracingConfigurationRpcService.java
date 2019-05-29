package com.vmturbo.components.common.logging;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.logging.Logging.GetTracingConfigRequest;
import com.vmturbo.common.protobuf.logging.Logging.GetTracingConfigResponse;
import com.vmturbo.common.protobuf.logging.Logging.SetTracingConfigRequest;
import com.vmturbo.common.protobuf.logging.Logging.SetTracingConfigResponse;
import com.vmturbo.common.protobuf.logging.Logging.TracingConfiguration;
import com.vmturbo.common.protobuf.logging.TracingConfigurationServiceGrpc.TracingConfigurationServiceImplBase;
import com.vmturbo.components.common.tracing.TracingManager;

public class TracingConfigurationRpcService extends TracingConfigurationServiceImplBase {

    private final TracingManager tracingManager;

    public TracingConfigurationRpcService(@Nonnull final TracingManager tracingManager) {
        this.tracingManager = tracingManager;
    }

    /**
     */
    public void getTracingConfig(GetTracingConfigRequest request,
                                 StreamObserver<GetTracingConfigResponse> responseObserver) {
        final TracingConfiguration tracingConfiguration = tracingManager.getTracingConfiguration();
        responseObserver.onNext(GetTracingConfigResponse.newBuilder()
            .setCurConfig(tracingConfiguration)
            .build());
        responseObserver.onCompleted();
    }

    /**
     */
    public void setTracingConfig(SetTracingConfigRequest request,
                                 StreamObserver<SetTracingConfigResponse> responseObserver) {
        final TracingConfiguration finalConfig =
            tracingManager.refreshTracingConfiguration(request.getNewConfig());
        responseObserver.onNext(SetTracingConfigResponse.newBuilder()
            .setResultConfig(finalConfig)
            .build());
        responseObserver.onCompleted();
    }
}
