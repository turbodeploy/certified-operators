package com.vmturbo.group;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.group.identity.IdentityProvider;

/**
 * The abstract class for updating discovered policies.
 */
public abstract class DiscoveredPolicyUpdater {
    private IdentityProvider identityProvider;
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
     * @param policyTypeLogging the type of policy (placement or setting). used only for logging.
     * @return the set of oids for policies that their associated target no longer discover
     * policies.
     */
    @Nonnull
    protected List<Long> findPoliciesAssociatedTargetsRemoved(
            @Nonnull Map<Long, Map<String, Long>> existingPolicies,
            @Nonnull Set<Long> targetOidsWithDiscoveredPolicies,
            @Nonnull Set<Long> undiscoveredTargets,
            @Nonnull String policyTypeLogging) {
        // remove the policies associated to targets that are no longer around
        Set<Long> targetRemoved = findTargetsRemoved(existingPolicies.keySet(),
            targetOidsWithDiscoveredPolicies, undiscoveredTargets);

        if (!targetRemoved.isEmpty()) {
            List<Long> policiesToRemove = new ArrayList<>();
            logger.info("Following targets have not reported any policies and therefore "
                + "associated {} policies to them will be removed: {}", policyTypeLogging, targetRemoved);
            for (Long targetId : targetRemoved) {
                final Map<String, Long> existingPolicyForTarget = existingPolicies.get(targetId);
                policiesToRemove.addAll(existingPolicyForTarget.values());
                logger.trace("The following {} policies are being removed the the target {}:"
                        + " {}", () -> policyTypeLogging,
                    () -> targetId, existingPolicyForTarget::keySet);
            }
            return policiesToRemove;
        } else {
            return Collections.emptyList();
        }
    }
}
