package com.vmturbo.topology.processor.targets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.identity.IdentityProvider;
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

    private static final String TARGET_PREFIX = "targets/";

    private final Logger logger = LogManager.getLogger();

    @GuardedBy("storeLock")
    private final KeyValueStore keyValueStore;
    private final IdentityProvider identityProvider;
    private final ProbeStore probeStore;

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
                        @Nonnull final IdentityProvider identityProvider,
                        @Nonnull final ProbeStore probeStore) {
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.probeStore = Objects.requireNonNull(probeStore);

        this.derivedTargetIdsByParentId = new ConcurrentHashMap<>();

        // Check the key-value store for targets backed up
        // by previous incarnations of a KVBackedTargetStore.
        final Map<String, String> persistedTargets = this.keyValueStore.getByPrefix(TARGET_PREFIX);

        this.targetsById = persistedTargets.entrySet().stream()
                .map(entry -> {
                    try {
                        final Target newTarget = new Target(entry.getValue());
                        addDerivedTargetRelationship(newTarget);
                        logger.info("Restored existing target " + newTarget.getId() + " for probe "
                                + newTarget.getProbeId());
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
        synchronized (storeLock) {
            // TODO: Add duplicate target check.
            final Target retTarget = new Target(identityProvider, probeStore, Objects.requireNonNull(spec));
            registerTarget(retTarget);
            return retTarget;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Target createTarget(long targetId, @Nonnull final TargetSpec spec) throws InvalidTargetException {
        synchronized (storeLock) {
            // TODO: Add duplicate target check.
            final Target retTarget = new Target(targetId, probeStore, Objects.requireNonNull(spec), false);
            registerTarget(retTarget);
            return retTarget;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createOrUpdateDerivedTargets(@Nonnull final List<TargetSpec> targetSpecs) {
        if (targetSpecs.isEmpty()) {
            return;
        }
        final long parentTargetId = targetSpecs.get(0).getParentId();
        final List<Target> derivedTargets = getDerivedTargets(parentTargetId);
        final List<Target> existingTargets = new ArrayList<>();
        for (final TargetSpec targetSpec : targetSpecs) {
            try {
                final ProbeInfo probeInfo = probeStore.getProbe(targetSpec.getProbeId())
                        .orElseThrow(() -> new InvalidTargetException("No probe found for probe id " +
                                targetSpec.getProbeId()));
                synchronized (storeLock) {
                    // TODO: replace to the duplicate filter method in identity service.
                    final Optional<Target> existingTargetOpt = getExistingTarget(targetSpec, derivedTargets,
                            probeInfo.getTargetIdentifierFieldList());
                    if (!existingTargetOpt.isPresent()) {
                        final Target retTarget = new Target(identityProvider, probeStore,
                                Objects.requireNonNull(targetSpec));
                        registerTarget(retTarget);
                    } else {
                        // Add the target to existingTargets list if it already exists, and update the
                        // existing target.
                        final Target existingTarget = existingTargetOpt.get();
                        updateTarget(existingTarget.getId(), targetSpec.getAccountValueList());
                        existingTargets.add(existingTarget);
                    }
                }
            } catch (TargetNotFoundException | InvalidTargetException e) {
                logger.error("Create or update derived target failed! {}", e);
            }
        }
        // Remove all the derived targets which are not in the latest response DTO.
        if (derivedTargets.removeAll(existingTargets)) {
            derivedTargets.forEach(target -> {
                try {
                    removeTarget(target.getId());
                } catch (TargetNotFoundException e) {
                    logger.error("Derived target {} was not found, {}", target.getId(), e);
                }
            });
        }
    }

    /**
     * Add parent id to derived target ids relationship into map.
     *
     * @param target The target which way add this relationship.
     */
    private void addDerivedTargetRelationship(@Nonnull final Target target) {
        if (target.getSpec().hasParentId()) {
            final long parentTargetId = target.getSpec().getParentId();
            derivedTargetIdsByParentId.computeIfAbsent(parentTargetId, k -> new HashSet<>())
                                        .add(target.getId());
        }
    }

    @GuardedBy("storeLock")
    private void registerTarget(Target target) {
        keyValueStore.put(TARGET_PREFIX + Long.toString(target.getId()), target.toJsonString());
        targetsById.put(target.getId(), target);
        addDerivedTargetRelationship(target);
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
            addDerivedTargetRelationship(retTarget);
        }

        logger.info("Updated target {} for probe {}", retTarget.getId(), retTarget.getProbeId());
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
     * @param targetId The id of the target which need to be removed.
     * @return Target The instance of removed target.
     * @throws TargetNotFoundException
     */
    private Target removeTarget(final long targetId) throws TargetNotFoundException {
        final Target oldTarget;
        synchronized (storeLock) {
            oldTarget = targetsById.remove(targetId);
            if (oldTarget == null) {
                throw new TargetNotFoundException(targetId);
            }
            // If it is a parent target, remove the relationship from derivedTargetIdsByParentId map and
            // all the derived targets.
            final Set<Long> derivedTargetIds = derivedTargetIdsByParentId.remove(targetId);
            if (derivedTargetIds != null) {
                for (final long derivedTargetId : derivedTargetIds) {
                    removeTarget(derivedTargetId);
                }
            }
            // If it is a derived target, remove the relationship from derivedTargetIdsByParentId map.
            if (oldTarget.getSpec().hasParentId()) {
                final long parentId = oldTarget.getSpec().getParentId();
                final Set<Long> childrenIds = derivedTargetIdsByParentId.get(parentId);
                // If we remove parent target first, this set will be null.
                if (childrenIds != null && childrenIds.remove(oldTarget.getId()) && childrenIds.isEmpty()) {
                    derivedTargetIdsByParentId.remove(parentId);
                }
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

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Target> getDerivedTargets(long parentTargetId) {
        final List<Target> derivedTargets = new ArrayList<>();
        synchronized (storeLock) {
            final Set<Long> derivedTargetIds = derivedTargetIdsByParentId.get(parentTargetId);
            if (derivedTargetIds != null) {
                final Iterator<Long> iter = derivedTargetIds.iterator();
                while (iter.hasNext()) {
                    final long derivedTargetId = iter.next();
                    final Optional<Target> derivedTarget = getTarget(derivedTargetId);
                    if (derivedTarget.isPresent()) {
                        derivedTargets.add(derivedTarget.get());
                    } else {
                        iter.remove();
                        logger.error("Derived target {} doesn't exist.", derivedTargetId);
                    }
                }
            }
        }
        return derivedTargets;
    }

    /**
     * Get the specified target if the target has alreadly exist. We compare values in target identifier
     * fields in TargetSpec with other existing targets's to see if they are all matched, then we can regard
     * the target as exist.
     *
     * @param targetSpec The target spec of current target.
     * @param targets The existing targets list.
     * @param targetIdentifierFields The target identifier fields that indicate the fields we should compare
     * to distinguish two targets.
     * @return Optional<Target> if the target already exist, then the Optional will contain the target, or empty.
     */
    @VisibleForTesting
    Optional<Target> getExistingTarget(@Nonnull final TargetSpec targetSpec,
            @Nonnull final Collection<Target> targets, @Nonnull final List<String> targetIdentifierFields) {
        final List<AccountValue> accountValues = targetSpec.getAccountValueList();
        for (final Target target : targets) {
            // For each target, we iterate all its account values, if the account value is belong to identifier
            // field, we compare this field with the one in target spec, the target we want to add, if the
            // target spec has this field and the string value is the same, we consider the identifier field
            // value is same, and we consider the target is the exist if all the identifier fields are same.
            int matchedFields = 0;
            for (final AccountValue accountValue : accountValues) {
                if (targetIdentifierFields.contains(accountValue.getKey())) {
                    final List<AccountValue> avList = target.getSpec().getAccountValueList().stream()
                            .filter(av -> av.getKey().equals(accountValue.getKey()))
                            .collect(Collectors.toList());
                    if (avList.size() != 1) {
                        logger.error("Account value {} has no or more than one fields in target {}.",
                                accountValue.getKey(), target.getId());
                        continue;
                    }
                    // Account value is not null and identifier field is matched.
                    if (accountValue.getStringValue() != null &&
                            avList.get(0).getStringValue().equals(accountValue.getStringValue())) {
                        matchedFields ++;
                    }
                }
            }
            // All identifier fields matched for the two targets, then return the existing target.
            if (matchedFields == targetIdentifierFields.size()) {
                return Optional.of(target);
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<Long> getProbeIdForTarget(final long targetId) {
        Optional<Target> optionalTarget = getTarget(targetId);
        if (optionalTarget.isPresent()) {
            long probeId = optionalTarget.get().getProbeId();
            return Optional.of(probeId);
        }
        return Optional.empty();
    }

    @Override
    public Optional<SDKProbeType> getProbeTypeForTarget(final long targetId) {
        Optional<Target> optionalTarget = getTarget(targetId);
        if (optionalTarget.isPresent()) {
            long probeId = optionalTarget.get().getProbeId();
            Optional<ProbeInfo> optionalProbeInfo = probeStore.getProbe(probeId);
            if (optionalProbeInfo.isPresent()) {
                return Optional.of(SDKProbeType.create(optionalProbeInfo.get().getProbeType()));
            }
        }
        return Optional.empty();
    }
}
