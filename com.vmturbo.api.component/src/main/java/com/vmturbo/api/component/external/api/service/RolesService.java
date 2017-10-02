package com.vmturbo.api.component.external.api.service;

import java.util.List;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.RoleApiDTO;
import com.vmturbo.api.serviceinterfaces.IRolesService;

/**
 * Service implementation of Roles
 **/
public class RolesService implements IRolesService {
    @Override
    public List<RoleApiDTO> getRoles() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public RoleApiDTO getRole(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }
}
