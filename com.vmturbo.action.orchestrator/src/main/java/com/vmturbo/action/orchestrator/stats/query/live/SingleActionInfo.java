package com.vmturbo.action.orchestrator.stats.query.live;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;

/**
 * Information about a single {@link ActionView} processed by the {@link CurrentActionStatReader}.
 * Mainly used to avoid calculating involved entities many times for each action.
 */
@Value.Immutable
public interface SingleActionInfo {

    ActionView action();

    /**
     * Caches the involved entities for each {@link InvolvedEntityCalculation} required.
     *
     * @return the involved entities for each {@link InvolvedEntityCalculation} required.
     */
    @Nonnull
    Map<InvolvedEntityCalculation, Set<ActionEntity>> involvedEntities();

}
