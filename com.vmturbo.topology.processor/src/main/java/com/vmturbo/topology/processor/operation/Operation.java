package com.vmturbo.topology.processor.operation;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.api.impl.OperationRESTApi.OperationDto;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * An {@link Operation} maintains the state of an ongoing operation
 * (e.g. discovery) the topology processor is running.
 */
@ApiModel("Operation")
public abstract class Operation {

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
     * The id of the probe that is performing the operation.
     */
    @ApiModelProperty(value = "The id of the probe that is performing the operation.", required = true)
    private final long probeId;

    /**
     * The time at which the operation started.
     */
    @ApiModelProperty(value = "The time at which the operation started.", required = true)
    private final LocalDateTime startTime;

    /**
     * The time at which the operation completed.
     * Initialized to null, and set to the current time when operation completes.
     */
    @ApiModelProperty(value = "The time at which the operation completed. Null or absent if the operation has not completed.", required = false)
    private LocalDateTime completionTime;

    /**
     * The status of the operation.
     */
    @ApiModelProperty(value = "The status of the operation.", required = true)
    private Status status;

    /**
     * If this is an user initiated operation or not.
     */
    @ApiModelProperty(value = "Whether the operation was triggered by the user", required = false)
    private boolean userInitiated;

    /**
     * Any errors that occurred with the operation.
     */
    @ApiModelProperty(value = "Any errors that occurred with the operation. The list will be empty if there have been no errors.", required = true)
    private List<String> errors;

    /**
     * Create a new operation.
     * The startTime will be set to the current time and status will be initialized to IN_PROGRESS.
     *
     * @param probeId The id of the probe that will be doing the operation.
     * @param targetId The id of the target being discovered.
     * @param identityProvider The identity provider to use to get an ID.
     */
    public Operation(final long probeId,
                     final long targetId,
                     @Nonnull final IdentityProvider identityProvider) {
        Objects.requireNonNull(identityProvider);
        this.startTime = LocalDateTime.now();
        this.id = identityProvider.generateOperationId();
        this.targetId = targetId;
        this.probeId = probeId;
        this.status = Status.IN_PROGRESS;
        this.errors = new ArrayList<>();
        this.userInitiated = false;
    }

    public long getId() {
        return id;
    }

    public long getTargetId() {
        return targetId;
    }

    public long getProbeId() {
        return probeId;
    }

    @Nonnull
    public LocalDateTime getStartTime() {
        return startTime;
    }

    @Nullable
    public LocalDateTime getCompletionTime() {
        return completionTime;
    }

    /**
     * Get the status of the operation.
     *
     * @return The status of the operation.
     */
    @Nonnull
    public Status getStatus() {
        return status;
    }

    @Nonnull
    public List<String> getErrors() {
        return errors;
    }

    /**
     * Check if the operation is in progress.
     *
     * @return True if the operation is in progress, false otherwise.
     */
    public boolean isInProgress() {
        return status == Status.IN_PROGRESS;
    }

    /**
     * Set if the operation is userInitiated.
     *
     * Set to true if the operation is triggered by user.
     *
     */
    public void setUserInitiated(boolean userInitiated) {
        this.userInitiated = userInitiated;
    }

    public boolean getUserInitiated() {
        return userInitiated;
    }

    /**
     * Set the status of the operation to be successful.
     * As a side-effect, the completion time will be set to now().
     *
     * @return A reference to {@link this} to support method chaining.
     */
    @Nonnull
    public Operation success() {
        status = Status.SUCCESS;
        completionTime = LocalDateTime.now();
        completeOperation();

        return this;
    }

    /**
     * Set the status of the operation to be failure.
     * As a side-effect, the completion time will be set to now().
     *
     * @return A reference to {@link this} to support method chaining.
     */
    @Nonnull
    public Operation fail() {
        status = Status.FAILED;
        completionTime = LocalDateTime.now();
        completeOperation();

        return this;
    }

    /**
     * Add an error.
     * Note that Status is NOT automatically transitioned to FAILED when errors are added.
     * The fail() method must be called independently to set status to FAILED if the error
     * has caused the operation to fail.
     *
     * @param error The error to add.
     * @return A reference to {@link this} to support method chaining.
     */
    @Nonnull
    public Operation addError(@Nonnull final ErrorDTO error) {
        errors.add(humanReadableError(error));
        return this;
    }

    /**
     * Add a list of errors. The errors will be translated to human readable strings from the DTOs.
     * Note that Status is NOT automatically transitioned to FAILED when errors are added.
     * The fail() method must be called independently to set status to FAILED if the error
     * has caused the operation to fail.
     *
     * @param errors The errors to add.
     * @return A reference to {@link this} to support method chaining
     */
    @Nonnull
    public Operation addErrors(@Nonnull final List<ErrorDTO> errors) {
        this.errors.addAll(errors.stream()
                .map(Operation::humanReadableError)
                .collect(Collectors.toList())
        );
        return this;
    }

    @Nonnull
    public OperationDto toDto() {
        final OperationDto result = new OperationDto(getId(), getTargetId(), getStartTime(),
                        getCompletionTime(), getStatus(), getErrors());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder(256);
        return builder.append(id).append(" ")
                .append(targetId).append(" ")
                .append(probeId).append(" ")
                .append(status).append(" ")
                .append(startTime).append(" ")
                .append(completionTime).append(" ")
                .append(errors)
                .toString();
    }

    /**
     * Generate a human-readable error message from an ErrorDTO protobuf message.
     *
     * @param error The ErrorDTO from which to generate a human readable message.
     * @return A human-readable error message generated from the ErrorDTO protobuf message.
     */
    public static String humanReadableError(@Nonnull ErrorDTO error) {
        return
            String.format("%s: %s", error.getSeverity(), error.getDescription())
                + humanReadableEntityAttribution(error);
    }

    /**
     * Get the timer for use in timing operation durations.
     *
     * @return The timer for use in timing operation durations.
     */
    @Nonnull
    protected abstract DataMetricTimer getMetricsTimer();

    /**
     * Get the counter that tracks the count of operation results.
     *
     * @return The counter that tracks the count of operation results.
     */
    @Nonnull
    protected abstract DataMetricCounter getStatusCounter();

    /**
     * Complete the operation and update any associated Prometheus metrics.
     */
    protected void completeOperation() {
        getMetricsTimer().observe();
        getStatusCounter().labels(status.name()).increment();
    }

    /**
     * Generate a human readable attribution for the entity associated with the error.
     *
     * @param error The entity to describe
     * @return A human readable attribution for the entity associated with the error
     *         Returns an empty string if the error has no entity attribution
     */
    private static String humanReadableEntityAttribution(@Nonnull ErrorDTO error) {
        return error.hasEntityUuid() ?
            String.format(" (%s %s)", error.getEntityType(), error.getEntityUuid()) :
            "";
    }
}
