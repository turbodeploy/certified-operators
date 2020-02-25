package com.vmturbo.api.component.external.api.util.action;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.enums.ActionDetailLevel;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;

/**
 * Common functionality that has to do with action searching.
 */
public class ActionSearchUtil {

    private final ActionsServiceBlockingStub actionOrchestratorRpc;
    private final ActionSpecMapper actionSpecMapper;
    private final PaginationMapper paginationMapper;
    private final SupplyChainFetcherFactory supplyChainFetcherFactory;
    private final long realtimeTopologyContextId;
    private final GroupExpander groupExpander;

    public ActionSearchUtil(
            @Nonnull ActionsServiceBlockingStub actionOrchestratorRpc,
            @Nonnull ActionSpecMapper actionSpecMapper,
            @Nonnull PaginationMapper paginationMapper,
            @Nonnull SupplyChainFetcherFactory supplyChainFetcherFactory,
            @Nonnull GroupExpander groupExpander,
            long realtimeTopologyContextId) {
        this.actionOrchestratorRpc = Objects.requireNonNull(actionOrchestratorRpc);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Get the actions related to the selected entity.
     *
     * @param scopeId entity selected as a scope.
     * @param inputDto query about the related actions.
     * @param paginationRequest pagination request.
     * @return a pagination response with {@link ActionApiDTO} objects.
     * @throws OperationFailedException if the call to the supply chain service failed
     * @throws InterruptedException if thread is interrupted during processing.
     * @throws UnsupportedActionException translation to {@link ActionApiDTO} object failed for one object,
     *                                    because of action type that is not supported by the translation.
     * @throws ExecutionException translation to {@link ActionApiDTO} object failed for one object.
     */
    @Nonnull
    public ActionPaginationResponse getActionsByEntity(
            @Nonnull ApiId scopeId,
            ActionApiInputDTO inputDto,
            ActionPaginationRequest paginationRequest)
            throws  InterruptedException, OperationFailedException,
                    UnsupportedActionException, ExecutionException {
        final Set<Long> scope = groupExpander.expandOids(ImmutableSet.of(scopeId.oid()));
        if (scope.isEmpty()) {
            return paginationRequest.finalPageResponse(Collections.emptyList(), 0);
        }

        final Set<Long> expandedScope;
        // if the field "relatedEntityTypes" is not empty, then we need to fetch additional
        // entities from the scoped supply chain
        if (inputDto != null &&
                inputDto.getRelatedEntityTypes() != null &&
                !inputDto.getRelatedEntityTypes().isEmpty()) {
            // get the scoped supply chain
            // extract entity oids from the supply chain and add them to the scope
            expandedScope = supplyChainFetcherFactory.expandScope(scope, inputDto.getRelatedEntityTypes());
        } else {
            // if there are no related entities, just get the aggregated entities, if they exist.
            expandedScope = supplyChainFetcherFactory.expandAggregatedEntities(scope);
        }

        if (!expandedScope.isEmpty()) {
            // create filter
            final ActionQueryFilter filter = actionSpecMapper.createActionFilter(inputDto,
                    Optional.of(expandedScope),
                    scopeId);

            // call the service and retrieve results
            final FilteredActionResponse response = actionOrchestratorRpc.getAllActions(
                    FilteredActionRequest.newBuilder()
                            .setTopologyContextId(realtimeTopologyContextId)
                            .setFilter(filter)
                            .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                            .build());

            // translate results
            ActionDetailLevel detailLevel = inputDto != null ? inputDto.getDetailLevel() : ActionDetailLevel.STANDARD;
            final List<ActionApiDTO> results = actionSpecMapper.mapActionSpecsToActionApiDTOs(
                    response.getActionsList().stream()
                        .map(ActionOrchestratorAction::getActionSpec)
                        .collect(Collectors.toList()),
                    realtimeTopologyContextId,
                    detailLevel);

            return PaginationProtoUtil.getNextCursor(response.getPaginationResponse())
                    .map(nextCursor -> paginationRequest.nextPageResponse(results, nextCursor, null))
                    .orElseGet(() -> paginationRequest.finalPageResponse(results, null));
        }

        return paginationRequest.finalPageResponse(Collections.emptyList(), null);
    }
}
