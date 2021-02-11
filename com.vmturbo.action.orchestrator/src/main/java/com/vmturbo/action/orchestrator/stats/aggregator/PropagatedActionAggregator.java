package com.vmturbo.action.orchestrator.stats.aggregator;

import static com.vmturbo.action.orchestrator.stats.aggregator.GlobalActionAggregator.GLOBAL_MGMT_UNIT_ID;
import static com.vmturbo.common.protobuf.action.InvolvedEntityExpansionUtil.EXPANSION_REQUIRED_ENTITY_TYPES;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.stats.ActionStat;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.store.InvolvedEntitiesExpander;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionEnvironmentType;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;

/**
 * An {@link PropagatedActionAggregator} for the propagated scope.
 *
 * <p>It's responsible for aggregating actions for the entities which are not involved to the actions
 * directly but the actions are propagated for them.</p>
 */
public class PropagatedActionAggregator extends ActionAggregatorFactory.ActionAggregator {

    private final InvolvedEntitiesExpander involvedEntitiesExpander;

    protected PropagatedActionAggregator(@Nonnull LocalDateTime snapshotTime,
                                         InvolvedEntitiesExpander involvedEntitiesExpander) {
        super(snapshotTime);
        this.involvedEntitiesExpander = involvedEntitiesExpander;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processAction(@Nonnull final StatsActionViewFactory.StatsActionView action,
                              @Nonnull final LiveActionsStatistician.PreviousBroadcastActions previousBroadcastActions) {
        final ActionEnvironmentType actionEnvType;
        try {
            actionEnvType = ActionEnvironmentType.forAction(action.recommendation());
        } catch (UnsupportedActionException e) {
            logger.warn("Attempted to process unsupported action!", e);
            return;
        }

        final EnvironmentTypeEnum.EnvironmentType envType
                        = actionEnvType == ActionEnvironmentType.CLOUD
                        ? EnvironmentTypeEnum.EnvironmentType.CLOUD
                        : EnvironmentTypeEnum.EnvironmentType.ON_PREM;

        Map<Integer, List<ActionDTO.ActionEntity>> actionEntitiesPerEntityType = new HashMap<>();
        EXPANSION_REQUIRED_ENTITY_TYPES.forEach(entityType -> {
            final List<ActionDTO.ActionEntity> actionEntities = action.involvedEntities().stream()
                            .filter(actionEntity -> involvedEntitiesExpander
                                            .shouldPropagateAction(actionEntity.getId(),
                                                                   Collections.singleton(
                                                                                   entityType)))
                            .collect(Collectors.toList());
            if (!actionEntities.isEmpty()) {
                actionEntitiesPerEntityType.put(entityType, actionEntities);
            }
        });

        if (actionEntitiesPerEntityType.isEmpty()) {
            logger.trace("No action entities found which should propagate action {}",
                         action.actionGroupKey());
            return;
        }
        // If at least one action entity should propagate actions then create an action stats record
        // for propagated entity types.
        logger.trace("Found action entities which will propagate action {}",
                     action.actionGroupKey());
        actionEntitiesPerEntityType.forEach((propagatedEntityType, propagatedActionEntities) -> {
            final MgmtUnitSubgroup.MgmtUnitSubgroupKey unitKey = ImmutableMgmtUnitSubgroupKey.builder()
                            .mgmtUnitId(GLOBAL_MGMT_UNIT_ID)
                            .mgmtUnitType(getManagementUnitType())
                            .environmentType(envType)
                            .entityType(propagatedEntityType)
                            .build();
            final ActionStat stat = getStat(unitKey, action.actionGroupKey());
            stat.recordAction(action.recommendation(), propagatedActionEntities,
                              actionIsNew(action, previousBroadcastActions));
        });
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
     * Factory class to {@link PropagatedActionAggregator}.
     */
    public static class PropagatedActionAggregatorFactory implements ActionAggregatorFactory<PropagatedActionAggregator> {

        private final InvolvedEntitiesExpander involvedEntitiesExpander;

        /**
         * Create {@link PropagatedActionAggregatorFactory} instance.
         * @param involvedEntitiesExpander involved entities expander.
         */
        public PropagatedActionAggregatorFactory(InvolvedEntitiesExpander involvedEntitiesExpander) {
            this.involvedEntitiesExpander = involvedEntitiesExpander;
        }

        @Override
        public PropagatedActionAggregator newAggregator(@Nonnull final LocalDateTime snapshotTime) {
            return new PropagatedActionAggregator(snapshotTime, involvedEntitiesExpander);
        }
    }
}
