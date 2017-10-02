package com.vmturbo.topology.processor.api.impl;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.topology.processor.api.DiscoveryStatus;
import com.vmturbo.topology.processor.api.OperationStatus;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.api.ValidationStatus;

/**
 * Wrapper for operation results.
 */
@Immutable
public class OperationStatusWrapper implements OperationStatus, DiscoveryStatus, ValidationStatus {

    private final TopologyProcessorDTO.OperationStatus operationStatus;

    public OperationStatusWrapper(@Nonnull TopologyProcessorDTO.OperationStatus operationStatus) {
        this.operationStatus = Objects.requireNonNull(operationStatus,
                        "Operation result DTO should not be null");
    }

    @Override
    public long getId() {
        return operationStatus.getId();
    }

    @Override
    public long getTargetId() {
        return operationStatus.getTargetId();
    }

    @Override
    public boolean isCompleted() {
        return operationStatus.getStatus() != Status.IN_PROGRESS;
    }

    @Override
    public boolean isSuccessful() {
        return operationStatus.getStatus() == Status.SUCCESS;
    }

    @Override
    public List<String> getErrorMessages() {
        return operationStatus.getErrorMessagesList();
    }

    @Override
    public Date getStartTime() {
        return new Date(operationStatus.getStartTime());
    }

    @Override
    public Date getCompletionTime() {
        return operationStatus.hasEndTime() ? new Date(operationStatus.getEndTime()) : null;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
                + "-"
                + operationStatus.getTargetId()
                + " "
                + Arrays.asList(operationStatus.toString().split("\n"));
    }
}
