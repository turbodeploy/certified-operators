package com.vmturbo.api.component.external.api.service;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.serviceinterfaces.IReservedInstancesService;

public class ReservedInstancesService implements IReservedInstancesService {

    @Override
    public List<ReservedInstanceApiDTO> getReservedInstances(@Nullable String scope) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReservedInstanceApiDTO getReservedInstanceByUuid(@Nonnull String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<EntityStatsApiDTO> getReservedInstancesStats(@Nonnull StatScopesApiInputDTO inputDto) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }
}