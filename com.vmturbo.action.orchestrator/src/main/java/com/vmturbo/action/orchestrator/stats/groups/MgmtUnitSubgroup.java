package com.vmturbo.action.orchestrator.stats.groups;

import java.util.Optional;

import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.aggregator.ActionAggregatorFactory.ActionAggregator;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;

/**
 * Identified a sub-group within a "management unit" for action statistics.
 *
 * A management unit is a collection of entities which users are likely to use to view and/or
 * manage their environment. We aggregate action stats by management unit so that historical
 * action stat breakdowns are available where they are most likely required/useful, without
 * overloading the system.
 *
 * A management unit is uniquely identified by its ID, but actions within the management unit
 * may be subdivided by entity type and/or environment type. How exactly that happens depends on
 * the action unit's {@link ActionAggregator}.
 */
@Value.Immutable
public interface MgmtUnitSubgroup {
    int id();

    MgmtUnitSubgroupKey key();

    @Value.Immutable
    interface MgmtUnitSubgroupKey {
        long mgmtUnitId();

        ManagementUnitType mgmtUnitType();

        EnvironmentType environmentType();

        /**
         * Unset if this {@link MgmtUnitSubgroup} represents "all" entities in the
         * management unit.
         */
        Optional<Integer> entityType();
    }
}
