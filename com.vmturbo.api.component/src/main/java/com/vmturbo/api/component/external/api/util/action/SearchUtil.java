package com.vmturbo.api.component.external.api.util.action;

import java.nio.file.AccessDeniedException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
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
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsResponse;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;

/**
 * Common functionality that has to do with searching.
 */
public class SearchUtil {
    private final Logger logger = LogManager.getLogger();

    private final SearchServiceBlockingStub searchServiceRpc;
    private final TopologyProcessor topologyProcessor;
    private final ActionsServiceBlockingStub actionOrchestratorRpc;
    private final ActionSpecMapper actionSpecMapper;
    private final PaginationMapper paginationMapper;
    private final SupplyChainFetcherFactory supplyChainFetcherFactory;
    private final long realtimeTopologyContextId;

    public SearchUtil(
            @Nonnull SearchServiceBlockingStub searchServiceRpc,
            @Nonnull TopologyProcessor topologyProcessor,
            @Nonnull ActionsServiceBlockingStub actionOrchestratorRpc,
            @Nonnull ActionSpecMapper actionSpecMapper,
            @Nonnull PaginationMapper paginationMapper,
            @Nonnull SupplyChainFetcherFactory supplyChainFetcherFactory,
            long realtimeTopologyContextId) {
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
        this.actionOrchestratorRpc = Objects.requireNonNull(actionOrchestratorRpc);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.realtimeTopologyContextId = Objects.requireNonNull(realtimeTopologyContextId);
    }

    /**
     * Fetch an entity in {@link TopologyEntityDTO} form, given its oid.
     *
     * @param oid the oid of the entity to fetch.
     * @return the entity in a {@link TopologyEntityDTO} format.
     *
     * @throws UnknownObjectException if the entity was not found.
     * @throws OperationFailedException if the operation failed.
     * @throws UnauthorizedObjectException if user does not have proper access privileges.
     * @throws InterruptedException if thread is interrupted during processing.
     * @throws AccessDeniedException if user is properly authenticated.
     */
    @Nonnull
    public TopologyEntityDTO getTopologyEntityDTO(long oid) throws Exception {
        // get information about this entity from the repository
        final SearchTopologyEntityDTOsResponse searchTopologyEntityDTOsResponse;
        try {
            searchTopologyEntityDTOsResponse =
                searchServiceRpc.searchTopologyEntityDTOs(
                    SearchTopologyEntityDTOsRequest.newBuilder()
                        .addEntityOid(oid)
                        .build());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
        if (searchTopologyEntityDTOsResponse.getTopologyEntityDtosCount() == 0) {
            final String message = "Error fetching entity with uuid: " + oid;
            logger.error(message);
            throw new UnknownObjectException(message);
        }
        return searchTopologyEntityDTOsResponse.getTopologyEntityDtos(0);
    }

    /**
     * Given a {@link TopologyEntityDTO} object, finds a discovering target
     * and returns it in {@link TargetApiDTO} format.  If no target is found,
     * {@code null} is returned.
     *
     * @param entityAsTopologyEntityDTO entity to find a discovering target for.
     * @throws CommunicationException communication with the topology processor failed.
     * @throws TopologyProcessorException mandatory data for the target could not be retrieved.
     * @return a discovering target.
     */
    @Nullable
    public TargetApiDTO fetchDiscoveringTarget(@Nonnull TopologyEntityDTO entityAsTopologyEntityDTO)
            throws CommunicationException, TopologyProcessorException {
        if (entityAsTopologyEntityDTO.hasOrigin()
                && entityAsTopologyEntityDTO.getOrigin().hasDiscoveryOrigin()
                && entityAsTopologyEntityDTO
                        .getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsCount() != 0) {

            // get the target that appears first in the list of discovering targets
            // TODO: fix this; logic to add the most appropriate target should be added
            final long targetId =
                entityAsTopologyEntityDTO.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIds(0);

            return fetchTarget(targetId);
        }
        return null;
    }

    /**
     * Given a {@link ServiceEntityApiDTO} object, finds a discovering target
     * and returns it in {@link TargetApiDTO} format.  If no target is found,
     * {@code null} is returned.
     *
     * @param serviceEntityApiDTO entity to find a discovering target for.
     * @throws CommunicationException communication with the topology processor failed.
     * @throws TopologyProcessorException mandatory data for the target could not be retrieved.
     * @return a discovering target.
     */
    @Nullable
    public TargetApiDTO fetchDiscoveringTarget(@Nonnull ServiceEntityApiDTO serviceEntityApiDTO)
            throws CommunicationException, TopologyProcessorException {
        final TargetApiDTO target = serviceEntityApiDTO.getDiscoveredBy();
        if (target != null) {
            final String targetId = target.getUuid();
            if (targetId != null) {
                return fetchTarget(Long.parseLong(targetId));
            }
        }
        return null;
    }

    /**
     * Given a target ID, fetches a corresponding {@link TargetApiDTO}.
     *
     * @param targetId of the target to be fetched
     * @throws CommunicationException communication with the topology processor failed.
     * @throws TopologyProcessorException mandatory data for the target could not be retrieved.
     * @return the target.
     */
    @Nonnull
    private TargetApiDTO fetchTarget(long targetId)
            throws CommunicationException, TopologyProcessorException {
        final TargetApiDTO target = new TargetApiDTO();
        target.setUuid(Long.toString(targetId));

        // fetch information about the target
        final TargetInfo targetInfo = topologyProcessor.getTarget(targetId);
        target.setDisplayName(
            TargetData.getDisplayName(targetInfo).orElseGet(() -> {
                logger.warn("Cannot find the display name of target with id {}", target::getUuid);
                return "";
            }));

        // fetch information about the probe, and store the probe type in the result
        final long probeId = targetInfo.getProbeId();
        final ProbeInfo probeInfo = topologyProcessor.getProbe(probeId);
        target.setType(probeInfo.getType());
        return target;
    }

    /**
     * Obtains an {@link ActionApiDTO} object and populates all "discovered by" fields
     * of all associated entities that it contains.
     *
     * @param actionApiDTO the {@link ActionApiDTO} object whose entities are to be populated
     *                     with target information.
     */
    public void populateActionApiDTOWithTargets(@Nonnull ActionApiDTO actionApiDTO) {
        Stream.of(actionApiDTO.getTarget(), actionApiDTO.getCurrentEntity(), actionApiDTO.getNewEntity())
                .filter(Objects::nonNull)
                .forEach(serviceEntityApiDTO -> {
                    final String entityId = serviceEntityApiDTO.getUuid();
                    final long entityIdNumber = Long.valueOf(entityId);
                    try {
                        serviceEntityApiDTO.setDiscoveredBy(
                            fetchDiscoveringTarget(getTopologyEntityDTO(entityIdNumber)));
                    } catch (Exception e) {
                        logger.warn(
                            "Cannot retrieve information about the discovering target " +
                                "of the entity with id {}: {}",
                            () -> entityId, e::toString);
                    }
                });
    }

    /**
     * Get the actions related to a set of entity uuids.
     *
     * @param entityUuids the set of entities.
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
            @Nonnull Set<Long> entityUuids,
            ActionApiInputDTO inputDto,
            ActionPaginationRequest paginationRequest)
            throws  InterruptedException, UnknownObjectException, OperationFailedException,
                    UnsupportedActionException, ExecutionException {
        final Set<Long> scope = new HashSet<>(entityUuids);

        // if the field "relatedEntityTypes" is not empty, then we need to fetch additional
        // entities from the scoped supply chain
        if (inputDto != null &&
                inputDto.getRelatedEntityTypes() != null &&
                !inputDto.getRelatedEntityTypes().isEmpty()) {
            // get the scoped supply chain
            // extract entity oids from the supply chain and add them to the scope
            scope.addAll(expandScope(entityUuids, inputDto.getRelatedEntityTypes()));
        }

        // create filter
        final ActionQueryFilter filter =
            actionSpecMapper.createActionFilter(inputDto, Optional.of(scope));

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
        results.forEach(this::populateActionApiDTOWithTargets);

        return
            PaginationProtoUtil
                .getNextCursor(response.getPaginationResponse())
                .map(nextCursor -> paginationRequest.nextPageResponse(results, nextCursor))
                .orElseGet(() -> paginationRequest.finalPageResponse(results));
    }

    @VisibleForTesting
    public Set<Long> expandScope(@Nonnull Set<Long> entityUuids, @Nonnull List<String> relatedEntityTypes)
            throws OperationFailedException {
        return
            supplyChainFetcherFactory.newNodeFetcher()
                .addSeedUuids(
                    entityUuids.stream().map(Object::toString).collect(Collectors.toList()))
                .entityTypes(relatedEntityTypes)
                .fetchEntityIds();
    }
}
