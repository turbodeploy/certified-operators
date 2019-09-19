package com.vmturbo.topology.processor.api.impl;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;

/**
 * Wrapper for target info for API.
 */
@Immutable
public class TargetInfoProtobufWrapper implements TargetInfo {

    private final TopologyProcessorDTO.TargetInfo targetInfo;

    public TargetInfoProtobufWrapper(@Nonnull TopologyProcessorDTO.TargetInfo targetInfo) {
        this.targetInfo = Objects.requireNonNull(targetInfo, "Target info should not be null");
    }

    @Override
    public Set<AccountValue> getAccountData() {
        return targetInfo.getSpec().getAccountValueList().stream()
                        .map(av -> new AccountValuesWrapper(av)).collect(Collectors.toSet());
    }

    @Override
    public long getId() {
        return targetInfo.getId();
    }

    @Override
    public long getProbeId() {
        return targetInfo.getSpec().getProbeId();
    }

    @Override
    public LocalDateTime getLastValidationTime() {
        return null;
    }

    @Override
    public String getStatus() {
        return null;
    }

    @Override
    public Optional<Long> getParentId() {
        return targetInfo.getSpec().hasParentId() ? Optional.of(targetInfo.getSpec().getParentId())
                : Optional.empty();
    }

    @Override
    public boolean isHidden() {
        return targetInfo.getSpec().getIsHidden();
    }

    @Override
    public boolean isReadOnly() {
        return targetInfo.getSpec().getReadOnly();
    }

    @Override
    public List<String> getDerivedTargetIds() {
        return targetInfo.getSpec().getDerivedTargetIdsList().stream().map(String::valueOf).collect(Collectors.toList());
    }
}
