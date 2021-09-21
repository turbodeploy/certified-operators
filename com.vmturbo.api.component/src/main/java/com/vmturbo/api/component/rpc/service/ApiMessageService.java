package com.vmturbo.api.component.rpc.service;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.api.ApiMessage.ActionConversionRequest;
import com.vmturbo.common.protobuf.api.ApiMessage.ActionConversionResponse;
import com.vmturbo.common.protobuf.api.ApiMessageServiceGrpc;

/**
 * The service for converting internal messages to API messages.
 */
public class ApiMessageService extends ApiMessageServiceGrpc.ApiMessageServiceImplBase {
    private static Logger logger = LogManager.getLogger();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ActionSpecMapper actionSpecMapper;
    private final long realtimeContextId;

    /**
     * Creates an instance of {@link ApiMessageService}.
     *
     * @param actionSpecMapper The mapper for converting from {@link ActionSpec}
     *                         to {@link ActionApiDTO}.
     * @param realtimeContextId the id for realtime context.
     */
    public ApiMessageService(@Nonnull ActionSpecMapper actionSpecMapper, long realtimeContextId) {
        this.actionSpecMapper = actionSpecMapper;
        this.realtimeContextId = realtimeContextId;
    }

    @Override
    public void convertActionToApiMessage(final ActionConversionRequest request,
                                          final StreamObserver<ActionConversionResponse> responseObserver) {
        final ActionSpec actionSpec = request.getActionSpec();
        try {
            final ActionApiDTO converted = actionSpecMapper
                    .mapActionSpecToActionApiDTO(actionSpec, realtimeContextId);
            final String serialized = OBJECT_MAPPER.writeValueAsString(converted);
            responseObserver.onNext(ActionConversionResponse
                    .newBuilder()
                    .setActionApiDto(serialized)
                    .build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            logger.error("Failed to convert the action with ID {}", actionSpec.getRecommendationId(), ex);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to convert the action with ID "
                            + actionSpec.getRecommendationId() + " with error " + ex.getMessage())
                    .asException());
        }
    }
}
