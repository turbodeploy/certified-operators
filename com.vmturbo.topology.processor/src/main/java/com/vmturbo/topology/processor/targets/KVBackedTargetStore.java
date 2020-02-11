package com.vmturbo.topology.processor.targets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.identity.store.IdentityStoreUpdate;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
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

    /**
     * A map of targets by id
     *
     * This is the primary source of truth for target data.
     *
     * This map is guarded--for write operations only--by the storeLock. For read operations,
     * we rely on the concurrency capabilities of the ConcurrentMap. Acquiring the storeLock
     * during read operations would likely cause a lot of contention for this lock.
     */
    private final ConcurrentMap<Long, Target> targetsById;

    @GuardedBy("storeLock")
    private final ConcurrentMap<Long, Set<Long>> derivedTargetIdsByParentId;

    /**
     * Map of derived target ID to set of targets that have that target ID as a derived target.
     */
    @GuardedBy("storeLock")
    private final ConcurrentMap<Long, Set<Long>> parentTargetIdsByDerivedTargetId;

    /**
     * Table that takes a parent ID and a derived target ID and returns the last target spec
     * returned by that parent target for that derived target.  We need this so that when one
     * parent of a derived target is deleted or no longer discovers that derived target, we can
     * get account values from another parent of that derived target.
     */
    @GuardedBy("storeLock")
    private final Table<Long, Long, TargetSpec> targetSpecByParentTargetIdDerivedTargetId;

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
        this.parentTargetIdsByDerivedTargetId = new ConcurrentHashMap<>();
        this.targetSpecByParentTargetIdDerivedTargetId = HashBasedTable.create();
        // Check the key-value store for targets backed up
        // by previous incarnations of a KVBackedTargetStore.
        final Map<String, String> persistedTargets = this.keyValueStore.getByPrefix(TARGET_KV_STORE_PREFIX);

        this.targetsById = persistedTargets.entrySet().stream()
            .map(entry -> {
                try {
                    final Target newTarget = new Target(entry.getValue(), probeStore);
                    // update data structures with parent - derived target relationship
                    newTarget.getSpec().getDerivedTargetIdsList().forEach(derivedTargetId ->
                        addDerivedTargetParent(newTarget.getId(), derivedTargetId,
                            Optional.empty()));
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
                try {
                    final Target retTarget = new Target(newTargetId, probeStore, spec, true);
                    registerTarget(retTarget);
                    return retTarget;
                } catch (InvalidTargetException e) {
                    // clean up identity store if we failed to create the Target
                    identityStore.removeItemOids(Sets.newHashSet(newTargetId));
                    throw e;
                }
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
    public void createOrUpdateDerivedTargets(@Nonnull final List<TargetSpec> targetSpecs,
                                             final long parentTargetId)
        throws IdentityStoreException {
       // All the derived target specs are guaranteed from the same parent target, so we can pick up the
        // parent id in the first element in the list.
        final Set<Long> existingDerivedTargetIds = new HashSet<>();
        synchronized (storeLock) {
            // Fetch the current derived target oids. We need to remove the existing targets which are not
            // occurred in this discovery cycle.
            existingDerivedTargetIds.addAll(getDerivedTargetIds(parentTargetId));
            if (!targetSpecs.isEmpty()) {
                final IdentityStoreUpdate<TargetSpec> identityStoreUpdate =
                    identityStore.fetchOrAssignItemOids(targetSpecs);
                // Save the created or new assigned oids which the derived target specs have into item maps.
                final Map<TargetSpec, Long> oldItems = identityStoreUpdate.getOldItems();
                final Map<TargetSpec, Long> newItems = identityStoreUpdate.getNewItems();
                // Iterate created oids and update the existing derived targets.
                oldItems.forEach((key, value) -> {
                    try {
                        addDerivedTargetParent(parentTargetId, value, Optional.of(key));
                        updateTarget(value, key.getAccountValueList());
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
                        addDerivedTargetParent(parentTargetId, value, Optional.of(key));
                        registerTarget(retTarget);
                    } catch (InvalidTargetException e) {
                        try {
                            // clean up identityStore if we failed to create the target
                            identityStore.removeItemOids(Sets.newHashSet(value));
                        } catch (IdentityStoreException ex) {
                            // should never happen
                            logger.error(String.format(
                                "Error cleaning up identity store after failed creation of target %s",
                                value), ex);
                        }
                        logger.error(String.format("Create new derived target %s failed!", value), e);
                    }
                });
            }
        }
        // Remove all the derived targets which are not in the latest response DTO.
        // Target deletion cannot happen within a storeLock, so we release the lock first.
        existingDerivedTargetIds.forEach(targetId -> {
            try {
                if (removeDerivedTargetFromParent(targetId, parentTargetId)) {
                    removeTarget(targetId);
                }
            } catch (TargetNotFoundException | InvalidTargetException e) {
                logger.error(String.format("Derived target %s was not found.", targetId), e);
            } catch (IdentityStoreException | IdentifierConflictException e) {
                logger.error(
                    String.format(
                        "Remove identifiers of target %s from database failed.", targetId), e);
            }
        });
        synchronized (storeLock) {
            try {
                updateDerivedTargetIds(parentTargetId);
            } catch (TargetNotFoundException e) {
                logger.error(String.format("Target %s was not found.", parentTargetId), e);
            }
        }
    }

    /**
     * Remove the relationships between a derived target and its parent.  Return true if the derived
     * target has no more parents.
     *
     * @param derivedTargetId the target id of the derived target.
     * @param parentTargetId  the target if of the parent target.
     * @return true if the derived target should be removed because it has no more parents.
     * @throws InvalidTargetException when updateTarget throws it.
     * @throws TargetNotFoundException when updateTarget throws it.
     * @throws IdentityStoreException when updateTarget throws it.
     * @throws IdentifierConflictException when updateTarget throws it.
     */
    private boolean removeDerivedTargetFromParent(final long derivedTargetId,
                                                  final long parentTargetId)
        throws InvalidTargetException, TargetNotFoundException,
        IdentityStoreException, IdentifierConflictException {
        synchronized (storeLock) {
            Set<Long> derivedTargets = derivedTargetIdsByParentId.get(parentTargetId);
            if (derivedTargets == null || !derivedTargets.remove(derivedTargetId)) {
                logger.warn("While removing derived target {} from parent {}: "
                        + "derived target id not found in parent derived target list.", derivedTargetId,
                    parentTargetId);
            }

            final Set<Long> ancestors = parentTargetIdsByDerivedTargetId.get(derivedTargetId);
            targetSpecByParentTargetIdDerivedTargetId.remove(parentTargetId, derivedTargetId);

            // Remove parent from ancestors and either update the the derived target with new account
            // values or mark it for removal.
            if (ancestors != null && ancestors.remove(parentTargetId)) {
                // no more ancestors, derived target should be deleted
                if (ancestors.isEmpty()) {
                    parentTargetIdsByDerivedTargetId.remove(derivedTargetId);
                    return true;
                }
                // we may have deleted the derived target whose account values we were using, so find
                // another set of account values to use
                updateTarget(derivedTargetId,
                    ancestors.stream()
                        .map(parentId ->
                            targetSpecByParentTargetIdDerivedTargetId.get(parentId, derivedTargetId))
                        .filter(Objects::nonNull)
                        .findFirst());
                return false;
            }
            // We should never get here.
            logger.warn("Attempt to remove derived target {} from parent {} but no existing " +
                "relationship found.", derivedTargetId, parentTargetId);
            return ancestors.isEmpty();
        }
    }

    /**
     * Called when we process a derived target spec from a parent target discovery and find that
     * the derived target already exists in the identity store.  In that case, we do some
     * bookkeeping here to track the relationship between the parent target and derived target.
     *
     * @param derivedTargetId target id of the derived target.
     * @param targetSpec TargetSpec of the derived target.
     * @param parentTargetId target id of the parent target.
     */
    @GuardedBy("storeLock")
    private void addDerivedTargetParent(final long parentTargetId,
                                        final long derivedTargetId,
                                        @Nonnull final Optional<TargetSpec> targetSpec) {
        Objects.requireNonNull(targetSpec);
        logger.debug("Adding target relationships between parent {} and derived target {}.",
            parentTargetId, derivedTargetId);
        parentTargetIdsByDerivedTargetId.computeIfAbsent(derivedTargetId,
            k -> new HashSet<>())
            .add(parentTargetId);
        derivedTargetIdsByParentId.computeIfAbsent(parentTargetId, k -> new HashSet<>())
            .add(derivedTargetId);
        targetSpec.ifPresent(spec ->
            targetSpecByParentTargetIdDerivedTargetId.put(parentTargetId, derivedTargetId, spec));
    }

    @GuardedBy("storeLock")
    private void registerTarget(Target target) {
        keyValueStore.put(TARGET_KV_STORE_PREFIX + Long.toString(target.getId()), target.toJsonString());
        targetsById.put(target.getId(), target);
        // update data structures with parent - derived target relationship
        target.getSpec().getDerivedTargetIdsList().forEach(derivedTargetId ->
            addDerivedTargetParent(target.getId(), derivedTargetId,
                Optional.empty()));

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

    /**
     * Convenience method to update a target when you may not have a TargetSpec.  If there is no
     * TargetSpec, get the account values from the existing target.
     *
     * @param targetId ID of target to update.
     * @param spec Optional TargetSpec with updated account values.
     * @return {@link Target} updated with the new state of the target.
     * @throws InvalidTargetException if underlying call to updateTarget throws.
     * @throws TargetNotFoundException if no target exists with targetId.
     * @throws IdentityStoreException if underlying call to updateTarget throws.
     * @throws IdentifierConflictException if underlying call to updateTarget throws.
     */
    private Target updateTarget(long targetId, @Nonnull Optional<TargetSpec> spec)
        throws InvalidTargetException, TargetNotFoundException,
            IdentityStoreException, IdentifierConflictException {
        final Target oldTarget = targetsById.get(targetId);
        if (oldTarget == null) {
            throw new TargetNotFoundException(targetId);
        }
        final List<AccountValue> accountValues = spec.map(TargetSpec::getAccountValueList)
            .orElse(oldTarget.getSpec().getAccountValueList());
        return updateTarget(targetId, accountValues);
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
            // Because derived targets' account values are reprocessed with every parent target
            // discovery, we are sending out spurious onTargetUpdated messages.  Curtail these
            // by skipping the update if nothing's changed.
            Set<AccountValue> oldAccountValSet = new HashSet<>(oldTarget.getSpec().getAccountValueList());
            Set<AccountValue> newAccountValSet = new HashSet<>(updatedFields);
            Set<Long> oldDerivedTargetIds = new HashSet<>(oldTarget.getSpec().getDerivedTargetIdsList());
            if (oldAccountValSet.equals(newAccountValSet) &&
                oldDerivedTargetIds.equals(getDerivedTargetIds(targetId))) {
                logger.debug("No change in account values or derived targets. "
                    + "Not updating target '{}' ({}).", oldTarget.getDisplayName(), targetId);
                return oldTarget;
            }
            retTarget =
                oldTarget.withUpdatedFields(updatedFields, probeStore)
                    .withUpdatedDerivedTargetIds(
                        getDerivedTargetIds(targetId)
                            .stream()
                            .collect(Collectors.toList()),
                        probeStore);
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
    @GuardedBy("storeLock")
    private void updateDerivedTargetIds(long targetId)
            throws TargetNotFoundException {
        final Target oldTarget = targetsById.get(targetId);
        if (oldTarget == null) {
            throw new TargetNotFoundException(targetId);
        }
        try {
            final Target retTarget = oldTarget.withUpdatedDerivedTargetIds(
                getDerivedTargetIds(targetId)
                    .stream()
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
        // if there were derived targets, we only remove those targets if they no longer have any
        // parent targets.  If the deleted target was the primary parent, but other parents exist,
        // we need to select a new primary parent and update the derived target with the new spec
        logger.info("Removing {} derived targets for target {}", derivedTargetIds.size(), targetId);
        derivedTargetIds.forEach(derivedTargetId -> {
            try {
                if (removeDerivedTargetFromParent(derivedTargetId, targetId)) {
                    removeTarget(derivedTargetId);
                }
            } catch (TargetNotFoundException | IdentityStoreException | InvalidTargetException
                | IdentifierConflictException e) {
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
