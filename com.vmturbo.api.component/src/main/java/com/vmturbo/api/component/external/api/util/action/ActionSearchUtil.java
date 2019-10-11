package com.vmturbo.api.component.external.api.util.action;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Common functionality that has to do with action searching.
 * TODO: Future refactoring should make this class obsolete.  OM-47354
 */
public class ActionSearchUtil {

    private final ActionsServiceBlockingStub actionOrchestratorRpc;
    private final ActionSpecMapper actionSpecMapper;
    private final PaginationMapper paginationMapper;
    private final SupplyChainFetcherFactory supplyChainFetcherFactory;
    private final long realtimeTopologyContextId;
    private final RepositoryApi repositoryApi;
    private final GroupExpander groupExpander;

    public ActionSearchUtil(
            @Nonnull ActionsServiceBlockingStub actionOrchestratorRpc,
            @Nonnull ActionSpecMapper actionSpecMapper,
            @Nonnull PaginationMapper paginationMapper,
            @Nonnull SupplyChainFetcherFactory supplyChainFetcherFactory,
            @Nonnull GroupExpander groupExpander,
            @Nonnull RepositoryApi repositoryApi,
            long realtimeTopologyContextId) {
        this.actionOrchestratorRpc = Objects.requireNonNull(actionOrchestratorRpc);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * If the given scope represents an aggregation, (Business Account, Region, or Availability Zone) we should
     * Retrieve actions associated with all nodes in the supply chain seeded by the scope UUID. Else, just get
     * actions associated with the provided scope.
     *
     * @param entityType for which associated actions are being fetched
     * @return whether or not to retrieve actions associated with all supply chain nodes
     */
    @Nonnull
    public boolean shouldGetSupplyChainNodeActions (int entityType) {
        switch (entityType) {
            // Cloud Aggregations
            case EntityType.BUSINESS_ACCOUNT_VALUE:
            case EntityType.REGION_VALUE:
            case EntityType.AVAILABILITY_ZONE_VALUE:
            // On-Prem Aggregations
            case EntityType.DATACENTER_VALUE:
            case EntityType.VIRTUAL_DATACENTER_VALUE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Get the actions related to a set of entity uuids.
     *
     * @param scopeIds the set of entities.
     * @param inputDto query about the related actions.
     * @param paginationRequest pagination request.
     * @return a pagination response with {@link ActionApiDTO} objects.
     * @throws UnknownObjectException if the entity or action was not found.
     * @throws OperationFailedException if the call to the supply chain service failed
     * @throws InterruptedException if thread is interrupted during processing.
     * @throws UnsupportedActionException translation to {@link ActionApiDTO} object failed for one object,
     *                                    because of action type that is not supported by the translation.
     * @throws ExecutionException translation to {@link ActionApiDTO} object failed for one object.
     */
    @Nonnull
    public ActionPaginationResponse getActionsByEntityUuids(
            @Nonnull Set<ApiId> scopeIds,
            ActionApiInputDTO inputDto,
            ActionPaginationRequest paginationRequest)
            throws  InterruptedException, UnknownObjectException, OperationFailedException,
                    UnsupportedActionException, ExecutionException {
        final Set<Long> expandedScope;
        // if the field "relatedEntityTypes" is not empty, then we need to fetch additional
        // entities from the scoped supply chain
        if (inputDto != null &&
                inputDto.getRelatedEntityTypes() != null &&
                !inputDto.getRelatedEntityTypes().isEmpty()) {
            final Set<Long> scope = groupExpander.expandOids(scopeIds.stream()
                .map(ApiId::oid)
                .collect(Collectors.toSet()));
            // get the scoped supply chain
            // extract entity oids from the supply chain and add them to the scope
            expandedScope = supplyChainFetcherFactory.expandScope(scope, inputDto.getRelatedEntityTypes());
        } else {
            // If the entity for which we're collecting actions represents an aggregation, (Account, Region,
            // Zone, DC, VDC) get actions for all nodes in the supply chain seeded from that UUID. Otherwise,
            // just get actions corresponding to that single entity
            final Set<Long> toNotExpand = new HashSet<>();
            final Set<Long> toExpand = new HashSet<>();
            scopeIds.forEach(scopeId -> {
                final long oid = scopeId.oid();
                final Optional<UIEntityType> scopeType = scopeId.getScopeType();
                if (scopeType.isPresent() && shouldGetSupplyChainNodeActions(scopeType.get().typeNumber())) {
                    toExpand.add(oid);
                } else {
                    toNotExpand.add(oid);
                }
            });
            expandedScope = groupExpander.expandOids(toNotExpand);
            if (toExpand.size() > 0) {
                expandedScope.addAll(supplyChainFetcherFactory.expandScope(toExpand, java.util.Collections.emptyList()));
            }
        }

        // create filter
        final ActionQueryFilter filter = actionSpecMapper.createActionFilter(inputDto,
            // if there are grouping entity like DataCenter, we should expand it to PMs to show
            // all actions for PMs in this DataCenter
            Optional.of(supplyChainFetcherFactory.expandGroupingServiceEntities(expandedScope)));

        // call the service and retrieve results
        final FilteredActionResponse response =
            actionOrchestratorRpc.getAllActions(
                FilteredActionRequest.newBuilder()
                    .setTopologyContextId(realtimeTopologyContextId)
                    .setFilter(filter)
                    .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                    .build());

        // translate results
        final List<ActionApiDTO> results =
            actionSpecMapper.mapActionSpecsToActionApiDTOs(
                response.getActionsList().stream()
                    .map(ActionOrchestratorAction::getActionSpec)
                    .collect(Collectors.toList()),
                realtimeTopologyContextId);

        return
            PaginationProtoUtil
                .getNextCursor(response.getPaginationResponse())
                .map(nextCursor -> paginationRequest.nextPageResponse(results, nextCursor))
                .orElseGet(() -> paginationRequest.finalPageResponse(results));
    }
}
