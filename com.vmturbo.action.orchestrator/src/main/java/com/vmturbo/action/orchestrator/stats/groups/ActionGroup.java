package com.vmturbo.action.orchestrator.stats.groups;

import org.immutables.value.Value;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;

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

        String getActionRelatedRisk();
    }
}
