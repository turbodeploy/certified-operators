package com.vmturbo.action.orchestrator.stats.groups;

import java.util.Set;

import org.immutables.value.Value;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.ActionGroupFilter;

/**
 * Identifies an "action group" for action statistics.
 * <p>
 * Within a management unit subgroup (see {@link MgmtUnitSubgroup}), actions are bisected by various
 * parameters. Each unique combination of these parameters forms an "action group".
 * <p>
 * For example - "Manual-mode in-progress resize actions recommended for efficiency purposes."
 * <p>
 * We use action groups because we need to capture the various ways the stats reader wants to
 * visualize the stats at write-time.
 */
@Value.Immutable
public interface ActionGroup {
    int id();

    ActionGroupKey key();

    @Value.Immutable
    interface ActionGroupKey {
        ActionType getActionType();

        ActionMode getActionMode();

        ActionCategory getCategory();

        ActionState getActionState();

        /**
         * The set of risks related to actions in this action group.
         *
         * <p/>Note: for action groups returned by {@link ActionGroupStore#query(ActionGroupFilter)}
         * this may be a subset of the original set of risks if the input {@link ActionGroupFilter}
         * restricted the desired risk types. This is to avoid having non-requested risks pop up
         * in the results.
         *
         * @return The set of risk strings.
         */
        Set<String> getActionRelatedRisk();
    }
}
