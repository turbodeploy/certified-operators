package com.vmturbo.topology.processor.topology.pipeline.blocking;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;

/**
 * Responsible for unblocking the {@link TopologyPipelineExecutorService} after the component
 * starts up using the provided {@link PipelineUnblock} operation
 *
 * <p/>It's extracted away from the executor service itself to avoid circular Spring dependencies.
 * between the unblock operations and
 */
public class PipelineUnblockLauncher implements RequiresDataInitialization {

    private final PipelineUnblock pipelineUnblock;

    private final TargetStore targetStore;

    PipelineUnblockLauncher(@Nonnull final PipelineUnblock pipelineUnblock,
            @Nonnull final TargetStore targetStore) {
        this.pipelineUnblock = pipelineUnblock;
        this.targetStore = targetStore;
    }

    @Override
    public void initialize() throws InitializationException {
        // Asynchronously wait for discoveries to complete before unblocking.
        //
        // We do this asynchronously because we don't want to wait until discoveries are done
        // to have the component up and responsive to other requests (e.g. add/remove targets).
        new Thread(pipelineUnblock, "wait-for-discoveries").start();
    }

    @Override
    public int priority() {
        // Initialize AFTER the target store, so that we can get accurate target information.
        return targetStore.priority() - 1;
    }
}
