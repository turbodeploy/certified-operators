package com.vmturbo.action.orchestrator.stats.aggregator;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.stats.ActionStat;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.ActionAggregatorFactory.ActionAggregator;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;

/**
 * Abstract class having shared logic for cloud aggregation (i.e., business account, resource
 * group).
 */
public abstract class CloudActionAggregator extends ActionAggregator {

    private final Predicate<EnvironmentType> environmentTypePredicate =
            EnvironmentTypeUtil.matchingPredicate(EnvironmentType.CLOUD);

    protected CloudActionAggregator(@Nonnull final LocalDateTime snapshotTime) {
        super(snapshotTime);
    }

    @Override
    public void processAction(@Nonnull final StatsActionViewFactory.StatsActionView action,
                              @Nonnull final LiveActionsStatistician.PreviousBroadcastActions previousBroadcastActions) {
        ActionDTO.ActionEntity primaryEntity = action.primaryEntity();
        // Don't even consider on-prem involved entities, because they won't be owned by
        // business accounts.
        if (environmentTypePredicate.test(primaryEntity.getEnvironmentType())) {
            // Find the most immediate business account owner.
            final Optional<Long> ownerOid = getAggregationEntity(action);
            if (ownerOid.isPresent()) {
                // Add the "global" action stats record.
                final MgmtUnitSubgroup.MgmtUnitSubgroupKey globalSubgroupKey = ImmutableMgmtUnitSubgroupKey.builder()
                        .mgmtUnitId(ownerOid.get())
                        .mgmtUnitType(getManagementUnitType())
                        // Business accounts are always in the cloud.
                        .environmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .build();
                final ActionStat stat = getStat(globalSubgroupKey, action.actionGroupKey());
                stat.recordAction(action.recommendation(), primaryEntity,
                        actionIsNew(action, previousBroadcastActions));
            } else {
                incrementEntityWithMissedAggregationEntity();
            }
        }
    }

    /**
     * Gets the oid for the entity that we are aggregating based on or empty if such entity does
     * not exists.
     *
     * @param action the action view object.
     * @return the oid of the aggregation entity or otherwise false.
     */
    protected abstract Optional<Long> getAggregationEntity(StatsActionViewFactory.StatsActionView action);

    /**
     * The method that increments the counter for the entities that don't have aggregation entity.
     */
    protected abstract void incrementEntityWithMissedAggregationEntity();
}
