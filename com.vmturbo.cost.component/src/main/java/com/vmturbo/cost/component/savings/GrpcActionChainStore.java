package com.vmturbo.cost.component.savings;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionChainsRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;

/**
 * Gets action chains from AO.
 */
public class GrpcActionChainStore implements ActionChainStore {
    /**
     * For querying action chain.
     */
    private final ActionsServiceBlockingStub actionServiceStub;

    /**
     * Comparator for specs, sorts by completion time.
     */
    @VisibleForTesting
    static final Comparator<ActionSpec> specComparator = (o1, o2) -> {
        if (o1.hasExecutionStep() && o2.hasExecutionStep()) {
            if (o1.getExecutionStep().hasCompletionTime()
                    && o2.getExecutionStep().hasCompletionTime()) {
                return (int)(o1.getExecutionStep().getCompletionTime()
                        - o2.getExecutionStep().getCompletionTime());
            }
        }
        return 0;
    };

    /**
     * Creates a new store.
     *
     * @param actionStub Action query stub.
     */
    public GrpcActionChainStore(@Nonnull final ActionsServiceBlockingStub actionStub) {
        actionServiceStub = actionStub;
    }

    @Override
    @Nonnull
    public Map<Long, NavigableSet<ActionSpec>> getActionChains(@Nonnull List<Long> entityIds) {
        final GetActionChainsRequest request = GetActionChainsRequest.newBuilder()
                .addAllEntityOid(entityIds)
                .build();
        Map<Long, NavigableSet<ActionSpec>> specsByEntity = new HashMap<>();
        actionServiceStub.getActionChains(request)
                .forEachRemaining(chain -> {
                    final long entityId = chain.getEntityOid();
                    if (!specsByEntity.containsKey(entityId)) {
                        specsByEntity.put(entityId, new TreeSet<>(specComparator));
                    }
                    for (ExecutedActionsChangeWindow window
                            : chain.getExecutedActionsChangeWindowList()) {
                        specsByEntity.get(entityId).add(window.getActionSpec());
                    }
                });
        return specsByEntity;
    }
}
