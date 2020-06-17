package com.vmturbo.topology.processor;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Abstract service implementation for action approval related operations.
 */
public abstract class AbstractActionApprovalService {

    private final Logger logger = LogManager.getLogger(getClass());

    private final TargetStore targetStore;

    /**
     * Constructs service.
     *
     * @param targetStore target store
     */
    protected AbstractActionApprovalService(@Nonnull TargetStore targetStore) {
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    /**
     * Returns id or a target, suitable for executing external approval related tasks.
     *
     * @return id, if any
     */
    @Nonnull
    protected Optional<Long> getTargetId() {
        for (Target target : targetStore.getAll()) {
            if (target.getProbeInfo().hasActionApproval()) {
                return Optional.of(target.getId());
            }
        }
        return Optional.empty();
    }

    @Nonnull
    protected Logger getLogger() {
        return logger;
    }
}
