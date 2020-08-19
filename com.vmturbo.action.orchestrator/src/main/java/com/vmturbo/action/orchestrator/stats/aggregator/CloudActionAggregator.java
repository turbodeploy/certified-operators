package com.vmturbo.action.orchestrator.stats.aggregator;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.stats.ActionStat;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.ActionAggregatorFactory.ActionAggregator;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;

/**
 * Abstract class having shared logic for cloud aggregation (i.e., business account, resource
 * group).
 */
public abstract class CloudActionAggregator extends ActionAggregator {
    protected CloudActionAggregator(@Nonnull final LocalDateTime snapshotTime) {
        super(snapshotTime);
    }

    @Override
    public void processAction(@Nonnull final StatsActionViewFactory.StatsActionView action,
                              @Nonnull final LiveActionsStatistician.PreviousBroadcastActions previousBroadcastActions) {
        // Initialize to empty map to avoid unnecessary object allocation for on-prem actions.
        Map<Long, List<ActionDTO.ActionEntity>> entitiesByOwnerAccount = Collections.emptyMap();
        for (ActionDTO.ActionEntity involvedEntity : action.involvedEntities()) {
            // Don't even consider on-prem involved entities, because they won't be owned by
            // business accounts.
            if (involvedEntity.getEnvironmentType() == EnvironmentTypeEnum.EnvironmentType.CLOUD || involvedEntity.getEnvironmentType() == EnvironmentTypeEnum.EnvironmentType.HYBRID) {
                // Find the most immediate business account owner.
                final Optional<Long> ownerOid = getAggregationEntity(involvedEntity.getId());
                if (ownerOid.isPresent()) {
                    // The first time we encounter an involved entity with an owner we can actually
                    // initialize the map.
                    if (entitiesByOwnerAccount.isEmpty()) {
                        entitiesByOwnerAccount = new HashMap<>(1);
                    }
                    entitiesByOwnerAccount.computeIfAbsent(ownerOid.get(),
                        k -> new ArrayList<>(action.involvedEntities().size()))
                        .add(involvedEntity);
                } else {
                    incrementEntityWithMissedAggregationEntity();
                }
            }
        }

        entitiesByOwnerAccount.forEach((accountId, entitiesForAccount) -> {
            // Add the "global" action stats record - all entities in this business account that are
            // involved in this action.
            final MgmtUnitSubgroup.MgmtUnitSubgroupKey globalSubgroupKey = ImmutableMgmtUnitSubgroupKey.builder()
                .mgmtUnitId(accountId)
                .mgmtUnitType(getManagementUnitType())
                // Business accounts are always in the cloud.
                .environmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                .build();
            final ActionStat stat = getStat(globalSubgroupKey, action.actionGroupKey());
            // Not using all entities involved in the snapshot, because some of them may be out
            // of the business account scope.
            stat.recordAction(action.recommendation(), entitiesForAccount, actionIsNew(action, previousBroadcastActions));
        });

    }

    /**
     * Gets the oid for the entity that we are aggregating based on or empty if such entity does
     * not exists.
     *
     * @param entityOid the oid of entity that aggregation entity is needed for.
     * @return the oid of the aggregation entity or otherwise false.
     */
    protected abstract Optional<Long> getAggregationEntity(long entityOid);

    /**
     * The method that increments the counter for the entities that don't have aggregation entity.
     */
    protected abstract void incrementEntityWithMissedAggregationEntity();
}
