package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.SuspensionMapper;
import com.vmturbo.api.dto.suspension.BulkActionRequestApiDTO;
import com.vmturbo.api.dto.suspension.BulkActionRequestInputDTO;
import com.vmturbo.api.dto.suspension.ScheduleEntityResponseApiDTO;
import com.vmturbo.api.dto.suspension.ScheduleTimeSpansApiDTO;
import com.vmturbo.api.dto.suspension.SuspendableEntityApiDTO;
import com.vmturbo.api.dto.suspension.SuspendableEntityInputDTO;
import com.vmturbo.api.dto.suspension.SuspendableEntityUUIDSetDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.ScheduleTimeSpansOrderBy;
import com.vmturbo.api.pagination.ScheduleTimeSpansPaginationRequest;
import com.vmturbo.api.pagination.ScheduleTimeSpansPaginationRequest.ScheduleTimeSpansPaginationResponse;
import com.vmturbo.api.pagination.SuspensionEntitiesPaginationRequest;
import com.vmturbo.api.pagination.SuspensionEntitiesPaginationRequest.SuspensionEntitiesPaginationResponse;
import com.vmturbo.api.serviceinterfaces.ISuspensionService;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityResponse;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityServiceGrpc.SuspensionEntityServiceBlockingStub;
import com.vmturbo.common.protobuf.suspension.SuspensionScheduleEntity.SuspensionAttachEntitiesResponse;
import com.vmturbo.common.protobuf.suspension.SuspensionScheduleEntity.SuspensionUpdateEntitiesResponse;
import com.vmturbo.common.protobuf.suspension.SuspensionScheduleEntityServiceGrpc.SuspensionScheduleEntityServiceBlockingStub;
import com.vmturbo.common.protobuf.suspension.SuspensionTimeSpanSchedule.DeleteTimespanScheduleResponse;
import com.vmturbo.common.protobuf.suspension.SuspensionTimeSpanSchedule.ListTimespanScheduleResponse;
import com.vmturbo.common.protobuf.suspension.SuspensionTimeSpanSchedule.TimespanSchedule;
import com.vmturbo.common.protobuf.suspension.SuspensionTimespanScheduleServiceGrpc.SuspensionTimespanScheduleServiceBlockingStub;
import com.vmturbo.common.protobuf.suspension.SuspensionToggle.SuspensionToggleEntityResponse;
import com.vmturbo.common.protobuf.suspension.SuspensionToggleServiceGrpc.SuspensionToggleServiceBlockingStub;

/**
 * SuspensionService.
 */
public class SuspensionService implements ISuspensionService {

    @Nonnull
    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    private final SuspensionEntityServiceBlockingStub entityService;

    @Nonnull
    private final SuspensionToggleServiceBlockingStub toggleService;

    @Nonnull
    private final SuspensionTimespanScheduleServiceBlockingStub timeSpanScheduleService;

    @Nonnull
    private final SuspensionScheduleEntityServiceBlockingStub suspensionScheduleEntityService;

    private final UserSessionContext userSessionContext;

    private final int apiPaginationDefaultLimit;

    private final int apiPaginationMaxLimit;

    /**
     * {@link SuspensionService} constructor.
     *
     * @param entityService RPC Schedule Service
     */
    public SuspensionService(@Nonnull final SuspensionEntityServiceBlockingStub entityService,
                             @Nonnull final SuspensionToggleServiceBlockingStub toggleService,
                             @Nonnull final SuspensionTimespanScheduleServiceBlockingStub timeSpanScheduleService,
                             @Nonnull final SuspensionScheduleEntityServiceBlockingStub suspensionScheduleEntityService,
                             @Nonnull final UserSessionContext userSessionContext,
                             @Nullable final int apiPaginationMaxLimit,
                             @Nullable final int apiPaginationDefaultLimit) {
        this.entityService = entityService;
        this.userSessionContext = userSessionContext;
        this.toggleService = toggleService;
        this.timeSpanScheduleService = timeSpanScheduleService;
        this.suspensionScheduleEntityService = suspensionScheduleEntityService;
        this.apiPaginationMaxLimit = apiPaginationMaxLimit;
        this.apiPaginationDefaultLimit = apiPaginationDefaultLimit;
    }

    @Override
    public List<BulkActionRequestApiDTO> bulkAction(BulkActionRequestInputDTO bulkActionRequestInputDTO) throws Exception {
        SuspensionMapper mapper = new SuspensionMapper();
        final SuspensionToggleEntityResponse response;
        try {
            response = toggleService.toggle(mapper.createSuspensionToggleEntityRequest(bulkActionRequestInputDTO));
        } catch (Exception e) {
            throw new OperationFailedException("Retrieval of toggle entities failed. ", e);
        }

        final List<BulkActionRequestApiDTO> data = mapper.convertToBulkActionRequestApiDTO(response.getDataList());

        return data;
    }

    @Override
    public SuspensionEntitiesPaginationResponse getEntities(SuspendableEntityInputDTO entityInputDTO, SuspensionEntitiesPaginationRequest paginationRequest) throws Exception {
        List<Long> scopeIDs = new ArrayList<Long>();
        if (userSessionContext.isUserScoped()) {
            for (Long oid : userSessionContext.getUserAccessScope().getScopeGroupIds()) {
                scopeIDs.add(oid);
            }
        }
        SuspensionMapper mapper = new SuspensionMapper(apiPaginationMaxLimit, apiPaginationDefaultLimit);
        SuspensionEntityResponse response;
        try {
            response = entityService.list(mapper.createSuspensionEntityRequestWithScope(entityInputDTO, paginationRequest, scopeIDs));
        } catch (Exception e) {
            if ((e instanceof StatusRuntimeException) && (e.getMessage().toLowerCase().contains("unknown scopes"))) {
                List<Long> resolvedEntityOIDs = new ArrayList<Long>();
                for (Long oid : userSessionContext.getUserAccessScope().accessibleOids()) {
                    resolvedEntityOIDs.add(oid);
                }
                try {
                    response = entityService.list(mapper.createSuspensionEntityRequestWithResolvedScopes(entityInputDTO, paginationRequest, resolvedEntityOIDs));
                } catch (Exception ex) {
                    throw new OperationFailedException("Retrieval of entities failed. runtime error", ex);
                }
            } else {
                throw new OperationFailedException("Retrieval of entities failed. ", e);
            }
        }
        final Integer totalRecordCount = response.getTotal();
        final List<SuspendableEntityApiDTO> entityApiDTOS = mapper.convertToSuspendableEntityApiDTO(response.getSuspensionEntitiesList());
        if (paginationRequest == null) {
                SuspensionEntitiesPaginationRequest entityPaginationRequest = new SuspensionEntitiesPaginationRequest();
                return entityPaginationRequest.allResultsResponse(entityApiDTOS);
        }
        if (response.getNextCursor() != null) {
            return  paginationRequest.nextPageResponse(entityApiDTOS, response.getNextCursor(), totalRecordCount);
        }
        return paginationRequest.finalPageResponse(entityApiDTOS, totalRecordCount);
    }

    /**
     * Add a new time span based schedule.
     *
     * @param schedule representing the new ScheduleTimeSpansApiDTO.
     * @return created ScheduleTimeSpansApiDTO object.
     * @throws Exception if any error happens
     */
    @Override
    public ScheduleTimeSpansApiDTO addTimeSpanSchedule(ScheduleTimeSpansApiDTO schedule) throws Exception {
        SuspensionMapper mapper = new SuspensionMapper();
        final TimespanSchedule response;
        try {
            response = timeSpanScheduleService.create(mapper.toCreateTimespanScheduleRequest(schedule));
        } catch (Exception e) {
            throw new OperationFailedException("creation of time span schedule failed. ", e);
        }
        return mapper.toApiScheduleTimeSpansApiDTO(response);
    }

    /**
     * Delete time span based schedule on basis of timeSpanSchedule_Uuid.
     *
     * @param timeSpanSchedule_Uuid This is representing the unique time span based schedule.
     * @throws Exception if any error happens
     */
    @Override
    public void deleteTimeSpanSchedule(String timeSpanSchedule_Uuid) throws Exception {
        SuspensionMapper mapper = new SuspensionMapper();
        final DeleteTimespanScheduleResponse response;
        try {
            timeSpanScheduleService.delete(mapper.toApiDeleteTimespanScheduleRequest(timeSpanSchedule_Uuid));
        } catch (Exception e) {
            throw new OperationFailedException("deletion of timespan schedule failed. ", e);
        }
    }

    /**
     * Update time span based schedule on basis of timeSpanSchedule_Uuid.
     *
     * @param uuid This is representing the unique time span based schedule.
     * @param schedule representing the updated ScheduleTimeSpansApiDTO.
     * @return updated ScheduleTimeSpansApiDTO object.
     * @throws Exception if any error happens
     */
    @Override
    public ScheduleTimeSpansApiDTO editTimeSpanSchedule(String uuid, ScheduleTimeSpansApiDTO schedule) throws Exception {
        SuspensionMapper mapper = new SuspensionMapper();
        final TimespanSchedule response;
        try {
            response = timeSpanScheduleService.update(mapper.toUpdateTimespanScheduleRequest(uuid, schedule));
        } catch (Exception e) {
            throw new OperationFailedException("updation of time span schedule failed. ", e);
        }
        return mapper.toApiScheduleTimeSpansApiDTO(response);
    }

    /**
     * List time span based schedules.
     *
     * @param paginationRequest representing the ScheduleTimeSpansPaginationRequest class.
     * @return list of ScheduleTimeSpansApiDTO object.
     * @throws Exception if any error happens
     */
    @Override
    public ScheduleTimeSpansPaginationResponse listTimespanSchedules(
            ScheduleTimeSpansPaginationRequest paginationRequest) throws Exception {
        SuspensionMapper mapper = new SuspensionMapper();
        final ListTimespanScheduleResponse response;
        try {
            response = timeSpanScheduleService.list(mapper.toListTimespanScheduleRequest(paginationRequest));
        } catch (Exception e) {
            throw new OperationFailedException("failed to fetch list of time span schedule. ", e);
        }
        final Integer totalRecordCount = response.getTotal();
        final List<ScheduleTimeSpansApiDTO> scheduleApiDTOS = new ArrayList<>();
        final Iterator<TimespanSchedule> iter =  response.getItemsList().iterator();
        while (iter.hasNext()) {
            scheduleApiDTOS.add(mapper.toApiScheduleTimeSpansApiDTO(iter.next()));
        }

        if (paginationRequest == null) {
            ScheduleTimeSpansPaginationRequest schedulePaginationRequest = new ScheduleTimeSpansPaginationRequest(null, 0, false,
                    ScheduleTimeSpansOrderBy.fromString(ScheduleTimeSpansOrderBy.DEFAULT));
            return schedulePaginationRequest.allResultsResponse(scheduleApiDTOS);
        }
        if (response.getNextCursor() != null) {
            return  paginationRequest.nextPageResponse(scheduleApiDTOS, response.getNextCursor(), totalRecordCount);
        }
        return paginationRequest.finalPageResponse(scheduleApiDTOS, totalRecordCount);
    }

    /**
     * Get time span based schedule on basis of timeSpanSchedule_Uuid.
     *
     * @param timeSpanSchedule_Uuid This is representing the unique time span based schedule.
     * @return ScheduleTimeSpansApiDTO the get schedule object.
     * @throws Exception if any error happens
     */
    @Override
    public ScheduleTimeSpansApiDTO getTimeSpanSchedule(String timeSpanSchedule_Uuid) throws Exception {
        SuspensionMapper mapper = new SuspensionMapper();
        final TimespanSchedule response;
        try {
            response = timeSpanScheduleService.get(mapper.toGetTimespanScheduleRequest(timeSpanSchedule_Uuid));
        } catch (Exception e) {
            throw new OperationFailedException("get time span based schedule failed. ", e);
        }
        return mapper.toApiScheduleTimeSpansApiDTO(response);
    }

    /**
     * Attach one or more suspendable entities to time span based schedule.
     *
     * @param timeSpanSchedule_Uuid uuid of the time span based schedule.
     * @param entityUuids suspendable entity uuids to .
     * @return list of ScheduleEntityResponseApiDTO class representing the partial failure per entity or success of the operation.
     * @throws Exception in case of error while attached suspendable entities to time span based schedule.
     */
    @Override
    public List<ScheduleEntityResponseApiDTO> attachTimeSpanSchedule(String timeSpanSchedule_Uuid, SuspendableEntityUUIDSetDTO entityUuids) throws Exception {
        SuspensionMapper mapper = new SuspensionMapper();
        final SuspensionAttachEntitiesResponse response;
        try {
            response = suspensionScheduleEntityService.attachEntities(mapper.toSuspensionAttachEntitiesRequest(timeSpanSchedule_Uuid, entityUuids));
        } catch (Exception e) {
            throw new OperationFailedException("attaching suspendable entities to time span schedule failed. ", e);
        }
        return mapper.toScheduleEntityResponseApiDTOForAttach(response);
    }

    /**
     * Replaces existing suspendable entities attached to time span based schedule.
     *
     * @param timeSpanSchedule_Uuid UUID of the time span schedule to replace existing suspendable entities attached.
     * @param entityUuids The SuspendableEntityUUIDSetDTO representing the array of suspendable entity_Uuid to replace the existing suspendable entities attached to time span based schedule.
     * @return list of ScheduleEntityResponseApiDTO class representing the partial failure per entity or success of the operation.
     * @throws Exception in case of error while replacing existing suspendable entities attached to a time span based schedule.
     */
    @Override
    public List<ScheduleEntityResponseApiDTO> updateTimeSpanScheduleAttachment(String timeSpanSchedule_Uuid, SuspendableEntityUUIDSetDTO entityUuids) throws Exception {
        SuspensionMapper mapper = new SuspensionMapper();
        final SuspensionUpdateEntitiesResponse response;
        try {
            response = suspensionScheduleEntityService.updateEntities(mapper.toSuspensionUpdateEntitiesRequest(timeSpanSchedule_Uuid, entityUuids));
        } catch (Exception e) {
            throw new OperationFailedException("replacing existing suspendable entities to time span schedule failed. ", e);
        }
        return mapper.toScheduleEntityResponseApiDTOForUpdate(response);
    }
}
