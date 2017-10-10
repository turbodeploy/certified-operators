package com.vmturbo.group.service;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
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
     * Gets all {@link SettingSpec} matching the criteria in the request.
     *
     * @param request SettingSpec request
     * @param responseObserver gRPC response observer
     */
    @Override
    public void searchSettingSpecs(SearchSettingSpecsRequest request,
                                   StreamObserver<SettingSpec> responseObserver) {

        logger.debug("Request: searchSettingSpec. Object: {}", request);

        // Store the requested names as a set for quicker comparisons.
        Optional<Set<String>> requestedNames = request.getSettingSpecNameCount() > 0 ?
                Optional.of(Sets.newHashSet(request.getSettingSpecNameList())) : Optional.empty();

        settingStore.getAllSettingSpec().stream()
                // If specific names are requested, filter out anything that doesn't match.
                .filter(spec -> requestedNames
                    .map(names -> names.contains(spec.getName()))
                    .orElse(true))
                .forEach(responseObserver::onNext);

        responseObserver.onCompleted();
    }
}
