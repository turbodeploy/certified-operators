package com.vmturbo.topology.processor.targets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.identity.store.IdentityStoreUpdate;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.topology.TopologyHandler;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.TopologyPipelineException;

/**
 * The consul-backed target store uses consul purely as a
 * backup for consistency across restarts.
 */
@ThreadSafe
public class KVBackedTargetStore implements TargetStore {

    private final Logger logger = LogManager.getLogger();

    @GuardedBy("storeLock")
    private final KeyValueStore keyValueStore;
    private final ProbeStore probeStore;
    private final IdentityStore<TargetSpec> identityStore;

    @GuardedBy("storeLock")
    private final ConcurrentMap<Long, Target> targetsById;

    @GuardedBy("storeLock")
    private final ConcurrentMap<Long, Set<Long>> derivedTargetIdsByParentId;

    /**
     * Locks for write operations on target storages.
     */
    private final Object storeLock = new Object();

    private final List<TargetStoreListener> listeners = Collections.synchronizedList(new ArrayList<>());

    public KVBackedTargetStore(@Nonnull final KeyValueStore keyValueStore,
                               @Nonnull final ProbeStore probeStore,
                               @Nonnull final IdentityStore<TargetSpec> identityStore) {

        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.identityStore = Objects.requireNonNull(identityStore);

        this.derivedTargetIdsByParentId = new ConcurrentHashMap<>();

        // Check the key-value store for targets backed up
        // by previous incarnations of a KVBackedTargetStore.
        final Map<String, String> persistedTargets = this.keyValueStore.getByPrefix(TARGET_KV_STORE_PREFIX);

        this.targetsById = persistedTargets.entrySet().stream()
            .map(entry -> {
                try {
                    final Target newTarget = new Target(entry.getValue(), probeStore);
                    addDerivedTargetsRelationships(newTarget);
                    addAccountDefEntryList(newTarget);
                    logger.info("Restored existing target '{}' ({}) for probe {}.", newTarget.getDisplayName(),
                            newTarget.getId(), newTarget.getProbeId());
                    return newTarget;
                } catch (TargetDeserializationException e) {
                    // It may make sense to delete the offending key here,
                    // but choosing not to do that for now to keep
                    // the constructor read-only w.r.t. the keyValueStore.
                    logger.warn("Failed to deserialize target: {}. Attempting to remove it.",
                        entry.getKey());
                    return null;
                } catch (TargetStoreException e) {
                    logger.error("Failed to deserialize target: " + entry.getKey(), e);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toConcurrentMap(Target::getId, Function.identity()));
    }

    /**
     * When deserializing a target, we need to add the {@link AccountDefEntry} from the related
     * probe as this is need by the target to determine if group scope is needed when returning
     * the list of {@link com.vmturbo.platform.common.dto.Discovery.AccountValue}s.
     *
     * @param newTarget the {@link Target} that we just deserialized.
     * @throws TargetStoreException if the {@link ProbeInfo} is not in the probe store.
     */
    private void addAccountDefEntryList(final Target newTarget) throws TargetStoreException {
        ProbeInfo probeInfo = probeStore.getProbe(newTarget.getProbeId())
            .orElseThrow(() ->
                new TargetStoreException("Probe information not found for target with id "
                    + newTarget.getProbeId()));
        newTarget.setAccountDefEntryList(probeInfo.getAccountDefinitionList());
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
    public Optional<String> getTargetDisplayName(long targetId) {
        return getTarget(targetId).map(Target::getDisplayName);
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
    public Target createTarget(@Nonnull final TargetSpec spec) throws InvalidTargetException,
        DuplicateTargetException, IdentityStoreException {
        synchronized (storeLock) {
            final IdentityStoreUpdate<TargetSpec> identityStoreUpdate = identityStore
                .fetchOrAssignItemOids(Arrays.asList(spec));
            final Map<TargetSpec, Long> oldItems = identityStoreUpdate.getOldItems();
            final Map<TargetSpec, Long> newItems = identityStoreUpdate.getNewItems();
            if (!newItems.isEmpty()) {
                final long newTargetId = newItems.values().iterator().next();
                final Target retTarget = new Target(newTargetId, probeStore, spec, true);
                registerTarget(retTarget);
                return retTarget;
            } else if (!oldItems.isEmpty()) {
                final long existingTargetId = oldItems.values().iterator().next();
                throw new DuplicateTargetException(getTargetDisplayName(existingTargetId)
                        .orElse(String.valueOf(existingTargetId)));
            }
            // Should never happen
            String targetDisplayName = Target.computeDisplayName(spec, probeStore);
            throw new IdentityStoreException(String.format("New target neither added nor retrieved: '%s'",
                    targetDisplayName));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Target restoreTarget(long targetId, @Nonnull final TargetSpec spec)
        throws InvalidTargetException {
        synchronized (storeLock) {
            final Target retTarget = new Target(targetId, probeStore, Objects.requireNonNull(spec), false);
            registerTarget(retTarget);
            return retTarget;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createOrUpdateDerivedTargets(@Nonnull final List<TargetSpec> targetSpecs)
        throws IdentityStoreException {
        if (targetSpecs.isEmpty()) {
            return;
        }
        // All the derived target specs are guaranteed from the same parent target, so we can pick up the
        // parent id in the first element in the list.
        final long parentTargetId = targetSpecs.get(0).getParentId();
        synchronized (storeLock) {
            // Fetch the current derived target oids. We need to remove the existing targets which are not
            // occurred in this discovery cycle.
            final Set<Long> existingDerivedTargetIds = getDerivedTargetIds(parentTargetId);
            final IdentityStoreUpdate<TargetSpec> identityStoreUpdate =
                    identityStore.fetchOrAssignItemOids(targetSpecs);
            // Save the created or new assigned oids which the derived target specs have into item maps.
            final Map<TargetSpec, Long> oldItems = identityStoreUpdate.getOldItems();
            final Map<TargetSpec, Long> newItems = identityStoreUpdate.getNewItems();
            // Iterate created oids and update the existing derived targets.
            oldItems.forEach((key, value) -> {
                try {
                    updateTarget(value, key.getAccountValueList());
                    updateDerivedTargetIds(value);
                    existingDerivedTargetIds.remove(value);
                } catch (InvalidTargetException | TargetNotFoundException |
                        IdentityStoreException | IdentifierConflictException e) {
                    logger.error(
                            String.format("Update derived target %s failed!", value), e);
                }
            });
            // Iterate new assigned oids and create new derived targets.
            newItems.forEach((key, value) -> {
                try {
                    final Target retTarget = new Target(value, probeStore,
                            Objects.requireNonNull(key), true);
                    registerTarget(retTarget);
                } catch (InvalidTargetException e) {
                    logger.error(String.format("Create new derived target %s failed!", value), e);
                }
            });
            // Remove all the derived targets which are not in the latest response DTO.
            existingDerivedTargetIds.forEach(targetId -> {
                try {
                    removeTarget(targetId);
                } catch (TargetNotFoundException e) {
                    logger.error(String.format("Derived target %s was not found.", targetId), e);
                } catch (IdentityStoreException e) {
                    logger.error(
                        String.format(
                            "Remove identifiers of target %s from database failed.", targetId), e);
                }
            });
            try {
                updateDerivedTargetIds(parentTargetId);
            } catch (TargetNotFoundException e) {
                logger.error(String.format("Target %s was not found.", parentTargetId), e);
            }
        }
    }

    /**
     * Add parent id to derived target ids relationship into map.
     *
     * @param target The target which way add this relationship.
     */
    private void addDerivedTargetsRelationships(@Nonnull final Target target) {
        if (target.getSpec().hasParentId()) {
            final long parentTargetId = target.getSpec().getParentId();
            derivedTargetIdsByParentId.computeIfAbsent(parentTargetId, k -> new HashSet<>())
                .add(target.getId());
        }
    }

    @GuardedBy("storeLock")
    private void registerTarget(Target target) {
        keyValueStore.put(TARGET_KV_STORE_PREFIX + Long.toString(target.getId()), target.toJsonString());
        targetsById.put(target.getId(), target);
        addDerivedTargetsRelationships(target);
        logger.info("Registered target '{}' ({}) for probe {}.", target.getDisplayName(), target.getId(),
                target.getProbeId());
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
        throws InvalidTargetException, TargetNotFoundException,
        IdentityStoreException, IdentifierConflictException {
        final Target retTarget;
        Objects.requireNonNull(updatedFields, "Fields should be specified");
        synchronized (storeLock) {
            final Target oldTarget = targetsById.get(targetId);
            if (oldTarget == null) {
                throw new TargetNotFoundException(targetId);
            }
            retTarget =
                    oldTarget.withUpdatedFields(updatedFields, probeStore);
            identityStore.updateItemAttributes(ImmutableMap.of(targetId, retTarget.getSpec()));
            targetsById.put(targetId, retTarget);
            keyValueStore.put(TARGET_KV_STORE_PREFIX + Long.toString(retTarget.getId()),
                retTarget.toJsonString());
        }

        logger.info("Updated target '{}' ({}) for probe {}", retTarget.getDisplayName(), targetId,
                retTarget.getProbeId());
        listeners.forEach(listener -> listener.onTargetUpdated(retTarget));
        return retTarget;
    }


    /**
     * Updates "derived target IDs" field for a given parent {@link Target}.
     * Maintains the logic of updating targets in the target store, identity store and consul.
     *
     * @param targetId parent {@link Target}'s ID.
     * @throws TargetNotFoundException When the requested target cannot be found.
     */
    private void updateDerivedTargetIds(long targetId)
            throws TargetNotFoundException {
        synchronized (storeLock) {
            final Target oldTarget = targetsById.get(targetId);
            if (oldTarget == null) {
                throw new TargetNotFoundException(targetId);
            }
            try {
                final Target retTarget = oldTarget.withUpdatedDerivedTargetIds(
                                getDerivedTargetIds(targetId)
                                        .stream()
                                        .map(String::valueOf)
                                        .collect(Collectors.toList()),
                                probeStore);
                identityStore.updateItemAttributes(ImmutableMap.of(targetId, retTarget.getSpec()));
                targetsById.put(targetId, retTarget);
                keyValueStore.put(TARGET_KV_STORE_PREFIX + Long.toString(retTarget.getId()),
                        retTarget.toJsonString());
            } catch (IdentityStoreException | IdentifierConflictException e) {
                logger.error(String.format(
                        "Remove identifiers of target %s from database failed.",
                        oldTarget.getId()), e);
            } catch (InvalidTargetException e) {
                logger.error(String.format("Target %s could not be created.", oldTarget.getId()), e);
            }
        }
    }

    /**
     * When a target has been removed, also trigger a broadcast
     * and reset broadcast schedule.
     */
    @Override
    public Target removeTargetAndBroadcastTopology(long targetId,
                                                   TopologyHandler topologyHandler,
                                                   Scheduler scheduler)
        throws TargetNotFoundException, IdentityStoreException {
        final Target oldTarget = removeTarget(targetId);
        try {
            topologyHandler.broadcastLatestTopology(StitchingJournalFactory.emptyStitchingJournalFactory());
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

    /**
     * Remove the target depend on the target id. If the target has derived targets, then we need to
     * remove the relationship in derivedTargetIdsByParentId and delete all children targets.
     *
     * @param targetId The id of the target which need to be removed.
     * @return Target The instance of removed target.
     * @throws TargetNotFoundException
     * @throws IdentityStoreException
     */
    private Target removeTarget(final long targetId) throws TargetNotFoundException, IdentityStoreException {
        final Target oldTarget;
        logger.info("Removing target {}", targetId);
        String targetName;
        synchronized (storeLock) {
            oldTarget = targetsById.remove(targetId);
            if (oldTarget == null) {
                throw new TargetNotFoundException(targetId);
            }
            targetName = oldTarget.getDisplayName();
            keyValueStore.removeKeysWithPrefix(TARGET_KV_STORE_PREFIX + Long.toString(targetId));
            identityStore.removeItemOids(ImmutableSet.of(targetId));
        }
        // Recursively remove any derived targets that are children of the target being removed
        // NOTE: This *must* occur outside of the synchronized block. Otherwise, the onTargetRemoved
        // listeners will be notified (within the recursive call) while the thread holds the storeLock.
        // This has been shown to lead to deadlock situations.
        removeDerivedTargetsRelationships(targetId);
        // Notify all listeners that the target has been removed. This must occur outside of the
        // synchronized block, else deadlocks are possible.
        logger.info("Removed target '{}' ({})", targetName, targetId);
        listeners.forEach(listener -> listener.onTargetRemoved(oldTarget));
        return oldTarget;
    }

    /**
     * Remove the target relationships with its parent target or derived targets. If the target has derived
     * targets, we need to remove all of them.
     *
     * @param targetId The target which we need to remove its derived targets or be
     */
    private void removeDerivedTargetsRelationships(@Nonnull final long targetId) {
        // If it is a parent target, remove the relationship from derivedTargetIdsByParentId map and
        // all the derived targets.
        final Set<Long> derivedTargetIds = derivedTargetIdsByParentId.remove(targetId);
        if (derivedTargetIds == null) {
            return;
        }
        logger.info("Removing {} derived targets for target {}", derivedTargetIds.size(), targetId);
        derivedTargetIds.forEach(derivedTargetId -> {
            try {
                removeTarget(derivedTargetId);
            } catch (TargetNotFoundException | IdentityStoreException e) {
                logger.error("Remove derived target " + derivedTargetId + " failed.", e);
            }
        });
    }

    @Override
    public void removeAllTargets() {
        getAll().stream().map(Target::getId).forEach(id -> {
            try {
                removeTarget(id);
            } catch (TargetNotFoundException | IdentityStoreException e) {
                // Not supposed to happen
                logger.error("Exception trying to remove target " + id, e);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Set<Long> getDerivedTargetIds(long parentTargetId) {
        final Set<Long> derivedTargetIds = new HashSet<>();
        synchronized (storeLock) {
            if (derivedTargetIdsByParentId.get(parentTargetId) != null) {
                derivedTargetIds.addAll(derivedTargetIdsByParentId.get(parentTargetId));
            }
        }
        return derivedTargetIds;
    }

    @Override
    public Optional<SDKProbeType> getProbeTypeForTarget(final long targetId) {
        Optional<Target> optionalTarget = getTarget(targetId);
        if (optionalTarget.isPresent()) {
            long probeId = optionalTarget.get().getProbeId();
            Optional<ProbeInfo> optionalProbeInfo = probeStore.getProbe(probeId);
            if (optionalProbeInfo.isPresent()) {
                return Optional.ofNullable(SDKProbeType.create(optionalProbeInfo.get().getProbeType()));
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<ProbeCategory> getProbeCategoryForTarget(final long targetId) {
        return getTarget(targetId)
            .flatMap(target -> probeStore.getProbe(target.getProbeId()))
            .flatMap(probeInfo -> Optional.ofNullable(ProbeCategory.create(probeInfo.getProbeCategory())));
    }
}
