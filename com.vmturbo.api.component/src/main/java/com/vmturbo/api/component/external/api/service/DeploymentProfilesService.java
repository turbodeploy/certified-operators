package com.vmturbo.api.component.external.api.service;

import java.util.List;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.deploymentprofile.DeploymentProfileApiDTO;
import com.vmturbo.api.dto.deploymentprofile.DeploymentProfileApiInputDTO;
import com.vmturbo.api.serviceinterfaces.IDeploymentProfilesService;

public class DeploymentProfilesService implements IDeploymentProfilesService {

    @Override
    public List<DeploymentProfileApiDTO> getDeploymentProfiles() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public DeploymentProfileApiDTO getDeploymentProfileByID(String id) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public DeploymentProfileApiDTO createDeploymentProfile(DeploymentProfileApiInputDTO deploymentProfileApiInputDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public DeploymentProfileApiDTO updateDeploymentProfile(String deploymentProfileID, DeploymentProfileApiInputDTO deploymentProfileApiInputDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public Boolean deleteDeploymentProfileByID(String id) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }
}
