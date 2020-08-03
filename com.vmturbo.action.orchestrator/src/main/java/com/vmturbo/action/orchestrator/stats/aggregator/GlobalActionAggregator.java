package com.vmturbo.action.orchestrator.stats.aggregator;

import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import com.vmturbo.action.orchestrator.stats.ActionStat;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician.PreviousBroadcastActions;
import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory.StatsActionView;
import com.vmturbo.action.orchestrator.stats.aggregator.ActionAggregatorFactory.ActionAggregator;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup.MgmtUnitSubgroupKey;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionEnvironmentType;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.proactivesupport.DataMetricCounter;

/**
 * An {@link ActionAggregator} for the global scope.
 */
public class GlobalActionAggregator extends ActionAggregator {

    /**
     * The global scope doesn't have a "real" ID, so we use a hard-coded value.
     */
    public static final long GLOBAL_MGMT_UNIT_ID = 0;

    private GlobalActionAggregator(@Nonnull final LocalDateTime snapshotTime) {
        super(snapshotTime);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processAction(@Nonnull final StatsActionView action,
                              @Nonnull final PreviousBroadcastActions previousBroadcastActions) {
        final ActionEnvironmentType actionEnvType;
        try {
            actionEnvType = ActionEnvironmentType.forAction(action.recommendation());
        } catch (UnsupportedActionException e) {
            logger.warn("Attempted to process unsupported action! Error: {}", e.getMessage());
            return;
        }

        final EnvironmentType envType;
        if (actionEnvType == ActionEnvironmentType.ON_PREM) {
            envType = EnvironmentType.ON_PREM;
        } else if (actionEnvType == ActionEnvironmentType.CLOUD) {
            envType = EnvironmentType.CLOUD;
        } else {
            // (roman, Jan 29 2019): We don't really expect this case to be hit - actions between
            // on-prem and cloud don't currently exist in the realtime case, so we don't account
            // for them.
            //
            // Note: Right now, since we don't expect "hybrid" environment actions, to see the
            // total actions in the environment we can add the ON_PREM and CLOUD. If we need to
            // support "hybrid" actions, this approach would lead to double-counting. We will need
            // to make the environment type of MgmtUnitSubgroupKey optional, and keep an extra
            // subgroup with an unset environment to represent total actions. Basically, do
            // what we do with entity types!
            envType = EnvironmentType.ON_PREM;
        }
        Metrics.ENV_TYPE_ACTIONS.labels(actionEnvType.name()).increment();

        // Update the per-entity-type records.
        final Multimap<Integer, ActionEntity> involvedEntitiesByType =
                Multimaps.index(action.involvedEntities(), ActionEntity::getType);
        involvedEntitiesByType.asMap().forEach((entityType, entities) -> {
            final MgmtUnitSubgroupKey unitKey = ImmutableMgmtUnitSubgroupKey.builder()
                .mgmtUnitId(GLOBAL_MGMT_UNIT_ID)
                .mgmtUnitType(getManagementUnitType())
                .environmentType(envType)
                .entityType(entityType)
                .build();
            final ActionStat stat = getStat(unitKey, action.actionGroupKey());
            stat.recordAction(action.recommendation(), entities,
                actionIsNew(action, previousBroadcastActions));
        });

        // Update the global records.
        // We can't reliably re-construct the global records from the per-entity-type records
        // because a single action may appear in a variable number of entity type records.
        final MgmtUnitSubgroupKey unitKey = ImmutableMgmtUnitSubgroupKey.builder()
                .mgmtUnitId(GLOBAL_MGMT_UNIT_ID)
                .environmentType(envType)
                .mgmtUnitType(getManagementUnitType())
                .build();
        final ActionStat stat = getStat(unitKey, action.actionGroupKey());
        stat.recordAction(action.recommendation(), involvedEntitiesByType.values(),
            actionIsNew(action, previousBroadcastActions));
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

    public static class Metrics {
        private static final DataMetricCounter ENV_TYPE_ACTIONS = DataMetricCounter.builder()
            .withName("ao_action_stat_env_type_actions_count")
            .withHelp("The number of actions processed by the stats framework with a specific environment type.")
            .withLabelNames("env")
            .build()
            .register();

    }
}
