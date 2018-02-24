package com.vmturbo.group.service;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SingleSettingSpecRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceImplBase;
import com.vmturbo.group.persistent.SettingSpecStore;
import com.vmturbo.group.persistent.SettingStore;

public class SettingRpcService extends SettingServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final SettingSpecStore settingSpecStore;

    private final SettingStore settingStore;

    public SettingRpcService(@Nonnull final SettingSpecStore settingSpecStore,
                             @Nonnull final SettingStore settingStore) {
        this.settingSpecStore = Objects.requireNonNull(settingSpecStore);
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
        Optional<SettingSpec> settingSpec = settingSpecStore.getSettingSpec(settingSpecName);

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

        settingSpecStore.getAllSettingSpecs().stream()
                // If specific names are requested, filter out anything that doesn't match.
                .filter(spec -> requestedNames
                    .map(names -> names.contains(spec.getName()))
                    .orElse(true))
                .forEach(responseObserver::onNext);

        responseObserver.onCompleted();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getGlobalSetting(GetSingleGlobalSettingRequest request,
                                  StreamObserver<GetGlobalSettingResponse> responseObserver) {

        if (!request.hasSettingSpecName()) {
            responseObserver.onCompleted();
            return;
        }

        try {
            Optional<Setting> setting = settingStore.getGlobalSetting(
                    request.getSettingSpecName());

            GetGlobalSettingResponse.Builder response = GetGlobalSettingResponse.newBuilder();
            setting.ifPresent(response::setSetting);
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (InvalidProtocolBufferException e) {
            responseObserver.onError(
                Status.INTERNAL.withDescription(e.getMessage()).asException());
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getMultipleGlobalSettings(GetMultipleGlobalSettingsRequest request,
                                  StreamObserver<Setting> responseObserver) {

        Set<String> requestedSettings = new HashSet<>();

        if (request.getSettingSpecNameCount() > 0 ) {
            requestedSettings.addAll(request.getSettingSpecNameList());
        }

        try {
            settingStore.getAllGlobalSettings()
                .stream()
                // If specific names are requested, filter out anything that doesn't match.
                .filter(setting ->
                            (requestedSettings.isEmpty() ||
                                requestedSettings.contains(setting.getSettingSpecName())))
                .forEach(responseObserver::onNext);

            responseObserver.onCompleted();
        } catch (DataAccessException | InvalidProtocolBufferException e) {
            responseObserver.onError(
                Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateGlobalSetting(UpdateGlobalSettingRequest request,
                                    StreamObserver<UpdateGlobalSettingResponse> responseObserver) {

        if (!request.hasSettingSpecName()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Setting name is missing")
                .asException());
            responseObserver.onCompleted();
            return;
        }

        try {
            settingStore.updateGlobalSetting(createSettingFromRequest(request));
            responseObserver.onNext(UpdateGlobalSettingResponse.newBuilder().build());
        } catch (DataAccessException e) {
            responseObserver.onError(
                Status.INTERNAL.withDescription(e.getMessage()).asException());
        }

        responseObserver.onCompleted();
    }

    private Setting createSettingFromRequest(UpdateGlobalSettingRequest request) {

        Setting.Builder settingBuilder = Setting.newBuilder();
        settingBuilder.setSettingSpecName(request.getSettingSpecName());

        switch (request.getValueCase()) {
            case BOOLEAN_SETTING_VALUE:
                settingBuilder.setBooleanSettingValue(request.getBooleanSettingValue());
                break;
            case NUMERIC_SETTING_VALUE:
                settingBuilder.setNumericSettingValue(request.getNumericSettingValue());
                break;
            case STRING_SETTING_VALUE:
                settingBuilder.setStringSettingValue(request.getStringSettingValue());
                break;
            case ENUM_SETTING_VALUE:
                settingBuilder.setEnumSettingValue(request.getEnumSettingValue());
                break;
            default:
                throw new IllegalArgumentException("Unknown value type " + request.getValueCase());
        }

        return settingBuilder.build();
    }
}
