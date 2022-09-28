package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.SuspensionMapper;
import com.vmturbo.api.dto.suspension.BulkActionRequestApiDTO;
import com.vmturbo.api.dto.suspension.BulkActionRequestInputDTO;
import com.vmturbo.api.dto.suspension.SuspendableEntityApiDTO;
import com.vmturbo.api.dto.suspension.SuspendableEntityInputDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.SuspensionEntitiesPaginationRequest;
import com.vmturbo.api.pagination.SuspensionEntitiesPaginationRequest.SuspensionEntitiesPaginationResponse;
import com.vmturbo.api.serviceinterfaces.ISuspensionService;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityOuterClass.SuspensionEntityResponse;
import com.vmturbo.common.protobuf.suspension.SuspensionEntityServiceGrpc.SuspensionEntityServiceBlockingStub;
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
                             @Nonnull final UserSessionContext userSessionContext,
                             @Nullable final int apiPaginationMaxLimit,
                             @Nullable final int apiPaginationDefaultLimit) {
        this.entityService = entityService;
        this.userSessionContext = userSessionContext;
        this.toggleService = toggleService;
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
}
