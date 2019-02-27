package com.vmturbo.action.orchestrator.stats.query.live;

import java.util.Set;

import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;

/**
 * Information about a single {@link ActionView} processed by the {@link CurrentActionStatReader}.
 * Mainly used to avoid calculating involved entities many times for each action.
 */
@Value.Immutable
public interface SingleActionInfo {

    ActionView action();

    Set<ActionEntity> involvedEntities();
}
