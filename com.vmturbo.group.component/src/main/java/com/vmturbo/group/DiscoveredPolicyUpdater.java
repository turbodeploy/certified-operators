package com.vmturbo.group;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Collections2;
import com.google.common.collect.Table;
import com.google.protobuf.AbstractMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.setting.CollectionsUpdate;

/**
 * The abstract class for updating discovered policies.
 *
 * @param <T> type of a spec representation for a policy (policy info)
 * @param <R> type of a policy to return
 * @param <D> type of a discovered policy
 */
public abstract class DiscoveredPolicyUpdater<R, T extends AbstractMessage, D> {
    private final IdentityProvider identityProvider;
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Constructs policies updater.
     *
     * @param identityProvider identity provider used to assign new OIDs for new policy items.
     */
    public DiscoveredPolicyUpdater(@Nonnull IdentityProvider identityProvider) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    protected IdentityProvider getIdentityProvider() {
        return identityProvider;
    }

    protected Logger getLogger() {
        return logger;
    }

    /**
     * Returns the targets that have been removed based set existing target oids and the targets
     * that have discovered policies and those targets that should be ignored.
     *
     * @param existingPoliciesTargets the set of target oids for existing policies.
     * @param targetOidsWithDiscoveredPolicies the set of target oids for discovered policies.
     * @param targetsToIgnore the set of targets that we didn't perform discovery on and
     *                        therefore should be ignored.
     * @return the set of policies to be removed.
     */
    @Nonnull
    private static Set<Long> findTargetsRemoved(@Nonnull Set<Long> existingPoliciesTargets,
                                                @Nonnull Set<Long> targetOidsWithDiscoveredPolicies,
                                                @Nonnull Set<Long> targetsToIgnore) {
        Set<Long> removedTargets = new HashSet<>(existingPoliciesTargets);
        removedTargets.removeAll(targetOidsWithDiscoveredPolicies);
        removedTargets.removeAll(targetsToIgnore);
        return removedTargets;
    }

    /**
     * Find the list of oids for policies that are associated with targets that topology
     * processor no longer send discovered policies for them.
     *
     * @param existingPolicies The map from target oid to a map of policy name to policy oid for
     *                         existing policies
     * @param targetOidsWithDiscoveredPolicies the set of target oids for targets that have
     *                                         discovered policies
     * @param undiscoveredTargets the set of targets that we didn't perform discovery on and
     *      *                        therefore should be ignored.
     * @return the set of oids for policies that their associated target no longer discover
     * policies.
     */
    @Nonnull
    private List<Long> findPoliciesAssociatedTargetsRemoved(
            @Nonnull Map<Long, Map<String, DiscoveredObjectVersionIdentity>> existingPolicies,
            @Nonnull Set<Long> targetOidsWithDiscoveredPolicies,
            @Nonnull Set<Long> undiscoveredTargets) {
        // remove the policies associated to targets that are no longer around
        Set<Long> targetRemoved = findTargetsRemoved(existingPolicies.keySet(),
            targetOidsWithDiscoveredPolicies, undiscoveredTargets);

        if (!targetRemoved.isEmpty()) {
            List<Long> policiesToRemove = new ArrayList<>();
            logger.info("Following targets have not reported any policies and therefore "
                + "associated {} policies to them will be removed: {}", getPolicyType(), targetRemoved);
            for (Long targetId : targetRemoved) {
                final Map<String, DiscoveredObjectVersionIdentity> existingPolicyForTarget = existingPolicies.get(targetId);
                policiesToRemove.addAll(Collections2.transform(existingPolicyForTarget.values(),
                        DiscoveredObjectVersionIdentity::getOid));
                logger.trace("The following {} policies are being removed the the target {}:"
                        + " {}", this::getPolicyType,
                    () -> targetId, existingPolicyForTarget::keySet);
            }
            return policiesToRemove;
        } else {
            return Collections.emptyList();
        }
    }

    private <T> void logDecisionsCounts(@Nonnull String prefix,
            @Nonnull Collection<PolicyDecision<T>> decisions) {
        int newPolicies = 0;
        int changed = 0;
        int unchanged = 0;
        int deleted = 0;
        for (PolicyDecision<?> decision: decisions) {
            if (decision.isDelete()) {
                if (decision.getRecordToCreate() == null) {
                    deleted++;
                } else {
                    changed++;
                }
            } else {
                if (decision.getRecordToCreate() == null) {
                    unchanged++;
                } else {
                    newPolicies++;
                }
            }
        }
        getLogger().info("{} {} new policies, {} changed, {} unchanged, {} removed",
                prefix, newPolicies, changed, unchanged, deleted);
    }

    @Nonnull
    protected CollectionsUpdate<R> analyzeAllPolicies(
            @Nonnull Map<Long, Map<String, DiscoveredObjectVersionIdentity>> existingPolicies,
            @Nonnull Map<Long, Collection<D>> discoveredPolicies,
            @Nonnull Table<Long, String, Long> groupNamesByTarget, Set<Long> undiscoveredTargets) {
        final Collection<PolicyDecision<R>> decisions = new ArrayList<>();
        for (Entry<Long, Collection<D>> entry: discoveredPolicies.entrySet()) {
            final long targetId = entry.getKey();
            decisions.addAll(analyzePolicies(targetId, entry.getValue(),
                    existingPolicies.getOrDefault(targetId, Collections.emptyMap()),
                    groupNamesByTarget.row(targetId)));
        }
        // remove the policies associated to targets that are no longer around
        final Set<Long> objectsToDelete = new HashSet<>(
                findPoliciesAssociatedTargetsRemoved(existingPolicies, discoveredPolicies.keySet(),
                        undiscoveredTargets));
        logger.info("Will remove {} policies from removed targets: {}", objectsToDelete.size(),
                objectsToDelete);
        decisions.stream().filter(PolicyDecision::isDelete).map(PolicyDecision::getOid).forEach(
                objectsToDelete::add);
        getLogger().trace("The following {} policies will be removed: {}", objectsToDelete::size,
                objectsToDelete::toString);
        final Collection<R> policiesToAdd = decisions.stream().map(
                PolicyDecision::getRecordToCreate).filter(Objects::nonNull).collect(
                Collectors.toList());
        logDecisionsCounts("Overall ", decisions);
        return new CollectionsUpdate<>(policiesToAdd, objectsToDelete);
    }

    /**
     * Returns a policy type to reflect in the logs.
     *
     * @return policy type string
     */
    @Nonnull
    protected abstract String getPolicyType();

    @Nonnull
    private Collection<PolicyDecision<R>> analyzePolicies(final long targetId,
            @Nonnull Collection<D> policyInfos,
            @Nonnull Map<String, DiscoveredObjectVersionIdentity> existingPolicies,
            @Nonnull Map<String, Long> groupOids) {
        final Function<D, Optional<T>> policyMapper = createPolicyMapper(groupOids, targetId);
        getLogger().trace("Existing policies for target {}: {}", targetId, existingPolicies);
        final Collection<PolicyDecision<R>> decisions = new ArrayList<>(policyInfos.size());
        for (D policyInfo : policyInfos) {
            analyzePolicy(policyInfo, policyMapper, existingPolicies, targetId).ifPresent(
                    decisions::add);
        }
        final Set<Long> policiesToDelete = new HashSet<>(
                Collections2.transform(existingPolicies.values(),
                        DiscoveredObjectVersionIdentity::getOid));
        policiesToDelete.removeAll(Collections2.transform(decisions, PolicyDecision::getOid));
        decisions.addAll(policiesToDelete.stream()
                .map(oid -> new PolicyDecision<R>(oid, true, null))
                .collect(Collectors.toList()));
        logDecisionsCounts("For target " + targetId, decisions);
        return decisions;
    }

    @Nonnull
    protected abstract Function<D, Optional<T>> createPolicyMapper(
            @Nonnull Map<String, Long> groupIds, long targetId);


    @Nonnull
    protected abstract Optional<PolicyDecision<R>> analyzePolicy(
            @Nonnull D discoveredPolicy,
            @Nonnull Function<D, Optional<T>> policyMapper,
            @Nonnull Map<String, DiscoveredObjectVersionIdentity> existingPolicies,
            long targetId);

    /**
     * CLass represents a decision: what to do with this policy.
     *
     * @param <T> type of a policy
     */
    protected static class PolicyDecision<T> {
        private final long oid;
        private final boolean delete;
        private final T recordToCreate;

        public PolicyDecision(long oid, boolean delete, @Nullable T recordToCreate) {
            this.oid = oid;
            this.delete = delete;
            this.recordToCreate = recordToCreate;
        }

        /**
         * Returns OID of the policy.
         *
         * @return oid
         */
        public long getOid() {
            return oid;
        }

        /**
         * Whether it is required to remove existing policy from the DB.
         *
         * @return whether to delete existing
         */
        public boolean isDelete() {
            return delete;
        }

        /**
         * Returns a record to create in the DB, if required. Could be {@code null} if record
         * addition is not required.
         *
         * @return data to add
         */
        @Nullable
        public T getRecordToCreate() {
            return recordToCreate;
        }
    }
}
