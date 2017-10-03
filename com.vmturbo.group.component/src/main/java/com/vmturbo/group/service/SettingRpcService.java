package com.vmturbo.group.service;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.setting.SettingProto.AllSettingSpecRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SingleSettingSpecRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceImplBase;
import com.vmturbo.group.persistent.SettingStore;

public class SettingRpcService extends SettingServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final SettingStore settingStore;

    public SettingRpcService(final SettingStore settingStore) {
        this.settingStore = Objects.requireNonNull(settingStore);
    }

    /**
     * Gets a specific {@link SettingSpec}.
     *
     * @param request SettingSpec request
     * @param responseObserver gRPC response observer
     */
    @Override
    public void getSettingSpec(SingleSettingSpecRequest request, StreamObserver<SettingSpec> responseObserver) {
        logger.debug("Request: getSettingSpec for name: {}", request.getSettingSpecName());

        String settingSpecName = request.getSettingSpecName();

        // check valid setting spec name
        if (settingSpecName == null) {
            String errorMessage = "Invalid request: Setting Spec name not specified";
            logger.error(errorMessage);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asRuntimeException());
            return;
        }

        // retrieve the spec from the store
        Optional<SettingSpec> settingSpec = settingStore.getSettingSpec(settingSpecName);

        // check if spec was found
        if (settingSpec.isPresent()) {
            responseObserver.onNext(settingSpec.get());
            responseObserver.onCompleted();
        } else {
            String errorMessage = "Setting Spec with name \"" + settingSpecName + "\" not found";
            logger.error(errorMessage);
            responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asRuntimeException());
        }
    }

    /**
     * Gets all {@link SettingSpec}.
     *
     * @param request SettingSpec request
     * @param responseObserver gRPC response observer
     */
    @Override
    public void getAllSettingSpec(AllSettingSpecRequest request, StreamObserver<SettingSpec> responseObserver) {

        logger.debug("Request: getAllSettingSpec");

        // retrieve all the specs from the store
        Collection<SettingSpec> settingSpecCollection = settingStore.getAllSettingSpec();

        for (SettingSpec settingSpec : settingSpecCollection) {
            responseObserver.onNext(settingSpec);
        }
        responseObserver.onCompleted();
    }
}
