package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.setting.SettingsPolicyApiDTO;
import com.vmturbo.api.serviceinterfaces.ISettingsPoliciesService;

/**
 * Implement SettingsPolicies API services.
 *
 * This class is a placeholder, for now.
 **/

@Component
public class SettingsPoliciesService implements ISettingsPoliciesService {
    /**
     * Get a list of settings policy
     *
     * @param onlyDefaults Show only the defaults.
     * @param entityTypes Filter the list by entity type.
     * @return
     * @throws Exception
     */
    @Override
    public List<SettingsPolicyApiDTO> getSettingsPolicies(boolean onlyDefaults, List<String> entityTypes)
        throws Exception {
        return new ArrayList<>();
    }

    @Override
    public SettingsPolicyApiDTO getSettingsPolicyByUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public SettingsPolicyApiDTO createSettingsPolicy(SettingsPolicyApiDTO settingPolicy) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public SettingsPolicyApiDTO editSettingsPolicy(String uuid, SettingsPolicyApiDTO settingPolicy) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public boolean deleteSettingsPolicy(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void validateInput(Object obj, Errors e) {
        throw ApiUtils.notImplementedInXL();
    }
}
