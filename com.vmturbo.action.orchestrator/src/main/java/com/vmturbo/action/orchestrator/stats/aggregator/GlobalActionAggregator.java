package com.vmturbo.action.orchestrator.stats.aggregator;

import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import com.vmturbo.action.orchestrator.stats.ActionStat;
import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.SingleActionSnapshotFactory.SingleActionSnapshot;
import com.vmturbo.action.orchestrator.stats.aggregator.ActionAggregatorFactory.ActionAggregator;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup.MgmtUnitSubgroupKey;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;

/**
 * An {@link ActionAggregator} for the global scope.
 */
public class GlobalActionAggregator extends ActionAggregator {

    /**
     * The global scope doesn't have a "real" ID, so we use a hard-coded value.
     */
    public static final long GLOBAL_MGMT_UNIT_ID = 0;

    GlobalActionAggregator(@Nonnull final LocalDateTime snapshotTime) {
        super(snapshotTime);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processAction(@Nonnull final SingleActionSnapshot action) {
        // TODO (roman, Nov 15 2018): We should add the environment type to the action.
        // For now - as a purely temporary measure - we use the presence of "savingsPerHour"
        // because we're not setting it in on-prem actions.
        final EnvironmentType envType = action.recommendation().hasSavingsPerHour() ?
                EnvironmentType.CLOUD : EnvironmentType.ON_PREM;

        // Update the per-entity-type records.
        final Multimap<Integer, ActionEntity> involvedEntitiesByType =
                Multimaps.index(action.involvedEntities(), ActionEntity::getType);
        involvedEntitiesByType.asMap().forEach((entityType, entities) -> {
            final MgmtUnitSubgroupKey unitKey = ImmutableMgmtUnitSubgroupKey.builder()
                    .mgmtUnitId(GLOBAL_MGMT_UNIT_ID)
                    .environmentType(envType)
                    .entityType(entityType)
                    .build();
            final ActionStat stat = getStat(unitKey, action.actionGroupKey());
            stat.recordAction(action.recommendation(), entities);
        });

        // Update the global records.
        // We can't reliably re-construct the global records from the per-entity-type records
        // because a single action may appear in a variable number of entity type records.
        final MgmtUnitSubgroupKey unitKey = ImmutableMgmtUnitSubgroupKey.builder()
                .mgmtUnitId(GLOBAL_MGMT_UNIT_ID)
                .environmentType(envType)
                .build();
        final ActionStat stat = getStat(unitKey, action.actionGroupKey());
        stat.recordAction(action.recommendation(), involvedEntitiesByType.values());
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    protected ManagementUnitType getManagementUnitType() {
        return ManagementUnitType.GLOBAL;
    }

    /**
     * Factory class to {@link GlobalActionAggregator}.
     */
    public static class GlobalAggregatorFactory implements ActionAggregatorFactory<GlobalActionAggregator> {

        @Override
        public GlobalActionAggregator newAggregator(@Nonnull final LocalDateTime snapshotTime) {
            return new GlobalActionAggregator(snapshotTime);
        }
    }
}
