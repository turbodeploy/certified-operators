package com.vmturbo.cost.component.savings;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

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
     * Comparator for ExecutedActionsChangeWindow, sorts by completion time.
     */
    @VisibleForTesting
    public static final Comparator<ExecutedActionsChangeWindow> changeWindowComparator =
            Comparator.comparingLong(changeWindow -> changeWindow.getActionSpec().getExecutionStep()
                    .getCompletionTime());

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
    public Map<Long, NavigableSet<ExecutedActionsChangeWindow>> getActionChains(@Nonnull Set<Long> entityIds) {
        final GetActionChainsRequest request = GetActionChainsRequest.newBuilder()
                .addAllEntityOid(entityIds)
                .build();
        final Map<Long, NavigableSet<ExecutedActionsChangeWindow>> chainByEntity = new HashMap<>();
        actionServiceStub.getActionChains(request).forEachRemaining(chain -> {
            final long entityId = chain.getEntityOid();
            NavigableSet<ExecutedActionsChangeWindow> actionSpecs = new TreeSet<>(
                    changeWindowComparator);
            actionSpecs.addAll(chain.getExecutedActionsChangeWindowList());
            chainByEntity.put(entityId, actionSpecs);
        });
        return chainByEntity;
    }
}
