package com.vmturbo.api.component.external.api.service;

import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.springframework.validation.Errors;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiInputDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.HierarchicalRelationship;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.pagination.EntityPaginationRequest;
import com.vmturbo.api.pagination.EntityPaginationRequest.EntityPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IBusinessUnitsService;

public class BusinessUnitsService implements IBusinessUnitsService {

    @Override
    public List<BusinessUnitApiDTO> getBusinessUnits(@Nullable BusinessUnitType type,
                    @Nullable String cloudType, @Nullable Boolean hasParent) throws Exception {
        // TODO OM-35804 implement required behavior
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public BusinessUnitApiDTO getBusinessUnitByUuid(final String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public BusinessUnitApiDTO createBusinessUnit(
            final BusinessUnitApiInputDTO businessUnitApiInputDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public BusinessUnitApiDTO editBusinessUnit(final String uuid,
                       final BusinessUnitApiInputDTO businessUnitApiInputDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public Boolean deleteBusinessUnit(final String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void validateInput(final Object o, final Errors errors) {
        throw ApiUtils.notImplementedInXL();
    }

    @Nonnull
    @Override
    public Collection<BusinessUnitApiDTO> getRelatedBusinessUnits(@Nonnull String uuid,
                    HierarchicalRelationship relationship) throws Exception {
        // TODO OM-35804 implement required behavior
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public EntityPaginationResponse getEntities(final String uuid,
                            final EntityPaginationRequest paginationRequest) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ActionPaginationResponse getActions(final String uuid,
                            final ActionApiInputDTO inputDto,
                            final ActionPaginationRequest paginationRequest) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getActionCountStatsByUuid(final String uuid,
                            final ActionApiInputDTO paginationRequest) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByQuery(final String uuid,
                            @Nullable final StatPeriodApiInputDTO statPeriodApiInputDTO)
            throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public SupplychainApiDTO getSupplychainByUuid(@Nonnull final String uuid,
                            @Nullable final List<String> types,
                            @Nullable final List<EntityState> entityStates,
                            @Nullable final EntityDetailType detailTypes,
                            @Nullable final Boolean healthSummary) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

}
