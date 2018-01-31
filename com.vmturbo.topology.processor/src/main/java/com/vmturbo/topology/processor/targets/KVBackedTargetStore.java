package com.vmturbo.topology.processor.targets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.topology.TopologyHandler;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.TopologyPipelineException;

/**
 * The consul-backed target store uses consul purely as a
 * backup for consistency across restarts.
 */
@ThreadSafe
public class KVBackedTargetStore implements TargetStore {

    private static final String TARGET_PREFIX = "targets/";

    private final Logger logger = LogManager.getLogger();

    @GuardedBy("storeLock")
    private final KeyValueStore keyValueStore;
    private final IdentityProvider identityProvider;
    private final ProbeStore probeStore;

    @GuardedBy("storeLock")
    private final ConcurrentMap<Long, Target> targetsById;
    /**
     * Locks for write operations on target storages.
     */
    private final Object storeLock = new Object();

    private final List<TargetStoreListener> listeners = Collections.synchronizedList(new ArrayList<>());

    public KVBackedTargetStore(@Nonnull final KeyValueStore keyValueStore,
                        @Nonnull final IdentityProvider identityProvider,
                        @Nonnull final ProbeStore probeStore) {
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.probeStore = Objects.requireNonNull(probeStore);

        // Check the key-value store for targets backed up
        // by previous incarnations of a KVBackedTargetStore.
        Map<String, String> persistedTargets = null;
        persistedTargets = this.keyValueStore.getByPrefix(TARGET_PREFIX);

        this.targetsById = persistedTargets.entrySet().stream()
                .map(entry -> {
                    try {
                        final Target newTarget = new Target(entry.getValue());
                        logger.info("Restored existing target " + newTarget.getId() + " for probe " + newTarget.getProbeId());
                        return newTarget;
                    } catch (TargetDeserializationException e) {
                        // It may make sense to delete the offending key here,
                        // but choosing not to do that for now to keep
                        // the constructor read-only w.r.t. the keyValueStore.
                        logger.warn("Failed to deserialize target: " + entry.getKey() + ". Attempting to remove it.");
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toConcurrentMap(Target::getId, Function.identity()));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<Target> getTarget(final long targetId) {
        return Optional.ofNullable(targetsById.get(targetId));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<String> getTargetAddress(final long targetId) {
        return getTarget(targetId)
            .flatMap(target -> target.getNoSecretDto().getSpec().getAccountValueList().stream()
                .filter(accountValue -> accountValue.getKey().equalsIgnoreCase(
                    PredefinedAccountDefinition.Address.name()))
                .map(AccountValue::getStringValue)
                .findFirst());
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<Target> getAll() {
        return ImmutableList.copyOf(targetsById.values());
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Target createTarget(@Nonnull final TargetSpec spec) throws InvalidTargetException {
        final Target retTarget = new Target(identityProvider, probeStore, Objects.requireNonNull(spec));
        registerTarget(retTarget);
        return retTarget;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Target createTarget(long targetId, @Nonnull final TargetSpec spec) throws InvalidTargetException {
        final Target retTarget = new Target(targetId, probeStore, Objects.requireNonNull(spec), false);
        registerTarget(retTarget);
        return retTarget;
    }

    private void registerTarget(Target target) {
        synchronized (storeLock) {
            keyValueStore.put(TARGET_PREFIX + Long.toString(target.getId()), target.toJsonString());
            targetsById.put(target.getId(), target);
        }
        logger.info("Created target " + target.getId() + " for probe " + target.getProbeId());
        listeners.forEach(listener -> listener.onTargetAdded(target));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<Target> getProbeTargets(final long probeId) {
        return targetsById.values().stream()
                .filter(target -> target.getProbeId() == probeId)
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addListener(@Nonnull TargetStoreListener listener) {
         listeners.add(Objects.requireNonNull(listener));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeListener(@Nonnull TargetStoreListener listener) {
        return listeners.remove(Objects.requireNonNull(listener));
    }

    @Override
    public Target updateTarget(long targetId, @Nonnull Collection<AccountValue> updatedFields)
                    throws InvalidTargetException, TargetNotFoundException {
        final Target retTarget;
        Objects.requireNonNull(updatedFields, "Fields should be specified");
        synchronized (storeLock) {
            final Target oldTarget = targetsById.get(targetId);
            if (oldTarget == null) {
                throw new TargetNotFoundException(targetId);
            }

            retTarget = oldTarget.withUpdatedFields(updatedFields, probeStore);
            targetsById.put(targetId, retTarget);

            keyValueStore.put(TARGET_PREFIX + Long.toString(retTarget.getId()),
                            retTarget.toJsonString());
        }

        logger.info("Updated target " + retTarget.getId() + " for probe " + retTarget.getProbeId());
        listeners.forEach(listener -> listener.onTargetUpdated(retTarget));
        return retTarget;
    }

    /**
     * When a target has been removed, also trigger a broadcast
     * and reset broadcast schedule.
     */
    @Override
    public Target removeTargetAndBroadcastTopology(long targetId,
                                                   TopologyHandler topologyHandler,
                                                   Scheduler scheduler)
            throws TargetNotFoundException {
        final Target oldTarget = removeTarget(targetId);
        try {
            topologyHandler.broadcastLatestTopology();
            scheduler.resetBroadcastSchedule();
        } catch (InterruptedException e) {
          // Although this broadcast is interrupted, it could be recovered on next scheduled
          // broadcast. Also since we don't expect the client to handle this exception, it does
          // not throw back to the client.
          Thread.currentThread().interrupt(); // set interrupt flag
          logger.error("Interruption during broadcast of latest topology.");
        } catch (TopologyPipelineException e) {
            logger.error("Could not send topology broadcast after removing target " + targetId, e);
        }
        return oldTarget;
    }

    private Target removeTarget(final long targetId) throws TargetNotFoundException {
        final Target oldTarget;
        synchronized (storeLock) {
            oldTarget = targetsById.remove(targetId);
            if (oldTarget == null) {
                throw new TargetNotFoundException(targetId);
            }
            keyValueStore.remove(TARGET_PREFIX + Long.toString(targetId));
        }

        logger.info("Removed target " + targetId);

        listeners.forEach(listener -> listener.onTargetRemoved(oldTarget));
        return oldTarget;
    }

    @Override
    public void removeAllTargets() {
        getAll().stream().map(Target::getId).forEach(id -> {
            try {
                removeTarget(id);
            } catch (TargetNotFoundException e) {
                // Not supposed to happen
                logger.error("Exception trying to remove target " + id);
            }
        });
    }
}
