package com.vmturbo.api.component.external.api.service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.springframework.validation.Errors;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiInputDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.serviceinterfaces.IBusinessUnitsService;

public class BusinessUnitsService implements IBusinessUnitsService {

    @Override
    public List<BusinessUnitApiDTO> getBusinessUnits() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<GroupApiDTO> getBusinessUnitGroups(final String uuid) throws Exception {
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
    public Collection<BusinessUnitApiDTO> getRelatedBusinessUnits(@Nonnull String uuid) {
        // TODO OM-35804 implement required behavior
        throw ApiUtils.notImplementedInXL();
    }

}
