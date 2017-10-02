package com.vmturbo.topology.processor.api.impl;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.swagger.annotations.ApiModelProperty;

import com.vmturbo.topology.processor.api.ActionStatus;
import com.vmturbo.topology.processor.api.DiscoveryStatus;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.api.ValidationStatus;

/**
 * REST Api DTOs for operations reporting.
 */
public class OperationRESTApi {

    /**
     * Class to hold operation result data.
     */
    @Immutable
    public static class OperationDto implements DiscoveryStatus, ValidationStatus, ActionStatus {

        /**
         * The id of the operation.
         */
        @ApiModelProperty(value = "The id of the operation.", required = true)
        private final long id;

        /**
         * The id of the target being discovered.
         */
        @ApiModelProperty(value = "The id of the target being discovered.", required = true)
        private final long targetId;

        /**
         * The time at which the operation started.
         */
        @ApiModelProperty(value = "The time at which the operation started.", required = true)
        private final LocalDateTime startTime;

        /**
         * The time at which the operation completed. Initialized to null, and set to the current
         * time when operation completes.
         */
        @ApiModelProperty(
                        value = "The time at which the operation completed. Null or absent if the operation has not completed.",
                        required = false)
        // @JsonFormat(pattern = "yyyy-MM-dd")
        private final LocalDateTime completionTime;

        /**
         * The status of the operation.
         */
        @ApiModelProperty(value = "The status of the operation.", required = true)
        private final Status status;

        /**
         * Any errors that occurred with the operation.
         */
        @ApiModelProperty(
                        value = "Any errors that occurred with the operation. The list will be empty if there have been no errors.",
                        required = true)
        private final List<String> errors;

        public OperationDto(long id, long targetId, @Nonnull LocalDateTime startTime,
                        @Nullable LocalDateTime completionTime, @Nonnull Status status,
                        @Nullable List<String> errors) {
            this.id = id;
            this.targetId = targetId;
            this.startTime = startTime;
            this.completionTime = completionTime;
            this.status = status;
            this.errors = errors == null ? null : Collections.emptyList();
        }

        protected OperationDto() {
            this.id = -1;
            this.targetId = -1;
            this.startTime = null;
            this.completionTime = null;
            this.status = null;
            this.errors = null;
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public long getTargetId() {
            return targetId;
        }

        @Override
        public boolean isSuccessful() {
            return status == Status.SUCCESS;
        }

        @Override
        public List<String> getErrorMessages() {
            return errors;
        }

        @Override
        public Date getStartTime() {
            return toEpochMillis(startTime);
        }

        @Override
        public Date getCompletionTime() {
            return toEpochMillis(completionTime);
        }

        public Status getStatus() {
            return status;
        }

        public List<String> getErrors() {
            return errors;
        }

        @Override
        public boolean isCompleted() {
            return status != Status.IN_PROGRESS;
        }
    }

    /**
     * Response to Operation-related REST API methods (Discovery, Validation, Action Execution).
     */
    @Immutable
    public static class OperationResponse {
        public final String error;

        public final OperationDto operation;

        @ApiModelProperty(required = true)
        public final Long targetId;

        private OperationResponse() {
            operation = null;
            error = null;
            targetId = null;
        }

        private OperationResponse(final OperationDto operation, final String error, long targetId) {
            this.operation = operation;
            this.error = error;
            this.targetId = targetId;
        }

        public boolean isSuccess() {
            return error == null;
        }

        @Nonnull
        public static OperationResponse success(@Nonnull final OperationDto discovery) {
            return new OperationResponse(discovery, null, discovery.getTargetId());
        }

        @Nonnull
        public static OperationResponse error(@Nonnull final String error, long targetId) {
            return new OperationResponse(null, error, targetId);
        }
    }

    /**
     * Response for discover all targets request.
     */
    public static class DiscoverAllResponse {

        @ApiModelProperty(value = "The list of responses", required = true)
        private final List<OperationResponse> responses;

        public DiscoverAllResponse() {
            responses = null;
        }

        public DiscoverAllResponse(@Nonnull List<OperationResponse> responses) {
            this.responses = Objects.requireNonNull(responses);
        }

        /**
         * Must be publicly exposed for swagger to generate the correct documentation.
         *
         * @return The list of target discoveries.
         */
        public List<OperationResponse> getResponses() {
            return ImmutableList.copyOf(responses);
        }

        /**
         * Use the response map to access discoveries indexed by target id.
         *
         * @return A map of targets indexed by target IDs.
         */
        @ApiModelProperty(hidden = true)
        public Map<Long, OperationResponse> getResponseMap() {
            final ImmutableMap.Builder<Long, OperationResponse> builder =
                            new ImmutableMap.Builder<>();
            responses.stream().forEach(response -> builder.put(response.targetId, response));
            return builder.build();
        }

    }

    /**
     * Response for validate all targets request.
     */
    public static class ValidateAllResponse {

        private final List<OperationResponse> responses;

        public ValidateAllResponse() {
            responses = null;
        }

        public ValidateAllResponse(@Nonnull List<OperationResponse> responses) {
            this.responses = Objects.requireNonNull(responses);
        }

        /**
         * Must be publicly exposed for swagger to generate the correct documentation.
         *
         * @return The list of target validations.
         */
        public List<OperationResponse> getResponses() {
            return ImmutableList.copyOf(responses);
        }

        /**
         * Use the response map to access validations indexed by target id.
         *
         * @return A map of targets indexed by target IDs.
         */
        @ApiModelProperty(hidden = true)
        public Map<Long, OperationResponse> getResponseMap() {
            final ImmutableMap.Builder<Long, OperationResponse> builder =
                            new ImmutableMap.Builder<>();
            responses.stream().forEach(response -> builder.put(response.targetId, response));
            return builder.build();
        }
    }

    /**
     * Converts local date time into long type.
     *
     * @param date source date
     * @return date representation in milliseconds
     */
    @Nullable
    private static Date toEpochMillis(@Nullable LocalDateTime date) {
        if (date == null) {
            return null;
        }
        return new Date(date.toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli());
    }

}
