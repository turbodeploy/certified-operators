package com.vmturbo.api.component.external.api.util.action;

import java.nio.file.AccessDeniedException;
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

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
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
    private static final Logger logger = LogManager.getLogger();

    private SearchUtil() {}

    /**
     * Fetch an entity in {@link TopologyEntityDTO} form, given its oid.
     *
     * @param searchServiceRpc search service.
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
    public static TopologyEntityDTO getTopologyEntityDTO(
            @Nonnull SearchServiceBlockingStub searchServiceRpc,
            long oid)
            throws Exception {
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
     * @param topologyProcessor topology processor interface to be used.
     * @param entityAsTopologyEntityDTO entity to find a discovering target for.
     * @throws CommunicationException communication with the topology processor failed.
     * @throws TopologyProcessorException mandatory data for the target could not be retrieved.
     * @return a discovering target.
     */
    @Nullable
    public static TargetApiDTO fetchDiscoveringTarget(
            @Nonnull TopologyProcessor topologyProcessor,
            @Nonnull TopologyEntityDTO entityAsTopologyEntityDTO)
            throws CommunicationException, TopologyProcessorException {
        if (entityAsTopologyEntityDTO.hasOrigin()
                && entityAsTopologyEntityDTO.getOrigin().hasDiscoveryOrigin()
                && entityAsTopologyEntityDTO
                        .getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsCount() != 0) {
            final TargetApiDTO target = new TargetApiDTO();

            // get the target that appears first in the list of discovering targets
            // TODO: fix this; logic to add the most appropriate target should be added
            final long targetId =
                entityAsTopologyEntityDTO.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIds(0);
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
        return null;
    }

    /**
     * Obtains an {@link ActionApiDTO} object and populates all "discovered by" fields
     * of all associated entities that it contains.
     *
     * @param topologyProcessor topology processor interface to be used.
     * @param searchService search service.
     * @param actionApiDTO the {@link ActionApiDTO} object whose entities are to be populated
     *                     with target information.
     */
    public static void populateActionApiDTOWithTargets(
            @Nonnull TopologyProcessor topologyProcessor,
            @Nonnull SearchServiceBlockingStub searchService,
            @Nonnull ActionApiDTO actionApiDTO) {
        Stream.of(actionApiDTO.getTarget(), actionApiDTO.getCurrentEntity(), actionApiDTO.getNewEntity())
                .filter(Objects::nonNull)
                .forEach(serviceEntityApiDTO -> {
                    final String entityId = serviceEntityApiDTO.getUuid();
                    final long entityIdNumber = Long.valueOf(entityId);
                    try {
                        serviceEntityApiDTO.setDiscoveredBy(
                            fetchDiscoveringTarget(
                                topologyProcessor,
                                getTopologyEntityDTO(searchService, entityIdNumber)));
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
     * @param actionOrchestratorRpc action orchestrator internal service to be used.
     * @param topologyProcessor topology processor interface to be used.
     * @param searchServiceRpc search service to be used.
     * @param actionSpecMapper action mapper to be used.
     * @param paginationMapper pagination mapper to be used.
     * @param realtimeTopologyContextId real time context id.
     * @param entityUuids the set of entities.
     * @param inputDto query about the related actions.
     * @param paginationRequest pagination request.
     * @return a pagination response with {@link ActionApiDTO} objects.
     * @throws UnknownObjectException if the entity or action was not found.
     * @throws InterruptedException if thread is interrupted during processing.
     * @throws UnsupportedActionException translation to {@link ActionApiDTO} object failed for one object,
     *                                    because of action type that is not supported by the translation.
     * @throws ExecutionException translation to {@link ActionApiDTO} object failed for one object.
     */
    @Nonnull
    public static ActionPaginationResponse getActionsByEntityUuids(
            @Nonnull ActionsServiceBlockingStub actionOrchestratorRpc,
            @Nonnull TopologyProcessor topologyProcessor,
            @Nonnull SearchServiceBlockingStub searchServiceRpc,
            @Nonnull ActionSpecMapper actionSpecMapper,
            @Nonnull PaginationMapper paginationMapper,
            long realtimeTopologyContextId,
            @Nonnull Optional<Set<Long>> entityUuids,
            ActionApiInputDTO inputDto,
            ActionPaginationRequest paginationRequest)
            throws  InterruptedException, UnknownObjectException,
                    UnsupportedActionException, ExecutionException {
        final ActionQueryFilter filter = actionSpecMapper.createActionFilter(inputDto, entityUuids);

        final FilteredActionResponse response =
            actionOrchestratorRpc.getAllActions(
                FilteredActionRequest.newBuilder()
                    .setTopologyContextId(realtimeTopologyContextId)
                    .setFilter(filter)
                    .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                    .build());
        final List<ActionApiDTO> results =
            actionSpecMapper.mapActionSpecsToActionApiDTOs(
                response.getActionsList().stream()
                    .map(ActionOrchestratorAction::getActionSpec)
                    .collect(Collectors.toList()),
                realtimeTopologyContextId);
        results.forEach(actionApiDTO ->
            SearchUtil.populateActionApiDTOWithTargets(topologyProcessor, searchServiceRpc, actionApiDTO));
        return
            PaginationProtoUtil
                .getNextCursor(response.getPaginationResponse())
                .map(nextCursor -> paginationRequest.nextPageResponse(results, nextCursor))
                .orElseGet(() -> paginationRequest.finalPageResponse(results));
    }
}
