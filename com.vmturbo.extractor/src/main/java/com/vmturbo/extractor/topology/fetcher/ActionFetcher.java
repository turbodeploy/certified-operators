package com.vmturbo.extractor.topology.fetcher;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import io.grpc.StatusRuntimeException;

import it.unimi.dsi.fastutil.longs.Long2IntArrayMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest.ActionQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse.TypeCase;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.topology.fetcher.GroupFetcher.GroupData;

/**
 * Fetch action count information from action orchestrator.
 */
public class ActionFetcher extends DataFetcher<Long2IntMap> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The action states which we care. This is based on the parameters in action stats API request
     * coming from UI.
     * Todo: revisit this list to ensure we are not missing any states like: QUEUED.
     */
    private static final Set<ActionState> INTERESTED_ACTION_STATES =
            ImmutableSet.of(ActionState.READY, ActionState.IN_PROGRESS);

    private final ActionsServiceBlockingStub actionService;

    private final GroupData groupData;

    private final long topologyContextId;

    /**
     * Create a new instance.
     *
     * @param actionService     action service endpoint
     * @param groupData         group data
     * @param timer             timer
     * @param consumer          fn to handle fetched action data
     * @param topologyContextId context id of topology
     */
    public ActionFetcher(@Nonnull ActionsServiceBlockingStub actionService,
            @Nonnull GroupData groupData,
            @Nonnull MultiStageTimer timer,
            @Nonnull Consumer<Long2IntMap> consumer,
            long topologyContextId) {
        super(timer, consumer);
        this.actionService = actionService;
        this.groupData = groupData;
        this.topologyContextId = topologyContextId;
    }

    @Override
    protected String getName() {
        return "Fetch action counts";
    }

    /**
     * Load actions information from action orchestrator and calculate counts for each entity
     * and group.
     *
     * @return map from entity/group id to its actions count
     */
    @Override
    protected Long2IntMap fetch() {
        final Long2IntMap actionCountByEntityOrGroup = new Long2IntArrayMap();
        final Long2ObjectMap<Set<Long>> actionsByEntityId = fetchAllActions();

        // add action count for entities
        actionsByEntityId.long2ObjectEntrySet().forEach(entry ->
                actionCountByEntityOrGroup.put(entry.getLongKey(), entry.getValue().size()));

        // add action count for groups
        groupData.getGroupToLeafEntityIds().long2ObjectEntrySet().forEach(entry -> {
            long count = entry.getValue().stream()
                    .map(entityId -> actionsByEntityId.getOrDefault((long)entityId, Collections.emptySet()))
                    .flatMap(Collection::stream)
                    // De-dupe the action ids.
                    .distinct()
                    .count();
            actionCountByEntityOrGroup.put(entry.getLongKey(), (int)count);
        });

        return actionCountByEntityOrGroup;
    }

    /**
     * Fetch all necessary actions from AO.
     *
     * @return all actions we care
     */
    private Long2ObjectMap<Set<Long>> fetchAllActions() {
        final Long2ObjectMap<Set<Long>> actionsByEntityId = new Long2ObjectArrayMap<>();
        final FilteredActionRequest request = FilteredActionRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .addActionQuery(ActionQuery.newBuilder().setQueryFilter(
                    ActionQueryFilter.newBuilder().addAllStates(INTERESTED_ACTION_STATES)))
                // stream all actions
                .setPaginationParams(PaginationParameters.newBuilder().setEnforceLimit(false))
                .build();
        try {
            Iterator<FilteredActionResponse> responseIterator = actionService.getAllActions(request);
            while (responseIterator.hasNext()) {
                FilteredActionResponse actionResponse = responseIterator.next();
                if (actionResponse.getTypeCase() == TypeCase.ACTION_CHUNK) {
                    actionResponse.getActionChunk().getActionsList().forEach(actionOrchestratorAction -> {
                        try {
                            ActionDTOUtil.getInvolvedEntityIds(
                                    actionOrchestratorAction.getActionSpec().getRecommendation())
                                    .forEach(involvedEntityId ->
                                            actionsByEntityId.computeIfAbsent((long)involvedEntityId, k -> new HashSet<>())
                                                    .add(actionOrchestratorAction.getActionId()));
                        } catch (UnsupportedActionException e) {
                            // this should not happen
                            logger.error("Unsupported action {}", actionOrchestratorAction, e);
                        }
                    });
                }
            }
        } catch (StatusRuntimeException e) {
            logger.error("Error retrieving actions from action orchestrator."
                    + " No action counts for this round of extraction. Error: {}", e.getMessage());
        }
        return actionsByEntityId;
    }
}
