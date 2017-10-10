package com.vmturbo.api.component.external.api.service;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.input.SettingApiInputDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.serviceinterfaces.ISettingsService;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;

/**
 * Service implementation of Settings
 **/
public class SettingsService implements ISettingsService {

    private final SettingServiceBlockingStub settingServiceBlockingStub;

    private final SettingsMapper settingsMapper;

    SettingsService(@Nonnull final SettingServiceBlockingStub settingServiceBlockingStub,
                    @Nonnull final SettingsMapper settingsMapper) {
        this.settingServiceBlockingStub = settingServiceBlockingStub;
        this.settingsMapper = settingsMapper;
    }

    @Override
    public List<SettingsManagerApiDTO> getSettings() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<SettingApiDTO> getSettingsByUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public SettingApiDTO getSettingByUuidAndName(String uuid, String name) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public SettingApiDTO putSettingByUuidAndName(String uuid, String name, SettingApiInputDTO setting) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<SettingsManagerApiDTO> getSettingsSpecs(final String managerUuid,
                                                        final String entityType,
                                                        final boolean isPlan) throws Exception {
        final Iterable<SettingSpec> specIt = () -> settingServiceBlockingStub.searchSettingSpecs(
                SearchSettingSpecsRequest.getDefaultInstance());

        final List<SettingSpec> specs = StreamSupport.stream(specIt.spliterator(), false)
                .filter(spec -> settingMatchEntityType(spec, entityType))
                .collect(Collectors.toList());
        if (managerUuid != null) {
            return settingsMapper.toManagerDto(specs, managerUuid)
                    .map(Collections::singletonList)
                    .orElse(Collections.emptyList());
        } else {
            return settingsMapper.toManagerDtos(specs);
        }
    }

    @VisibleForTesting
    static boolean settingMatchEntityType(@Nonnull final SettingSpec settingSpec,
                                          @Nullable String entityType) {
        if (entityType == null) {
            return true;
        }

        // We consider global settings to not match any specific entity type.
        if (!settingSpec.hasEntitySettingSpec() ||
            !settingSpec.getEntitySettingSpec().hasEntitySettingScope()) {
            return false;
        }

        final int targetEntityType = ServiceEntityMapper.fromUIEntityType(entityType);
        EntitySettingScope scope = settingSpec.getEntitySettingSpec().getEntitySettingScope();

        return scope.hasAllEntityType() ||
            scope.getEntityTypeSet().getEntityTypeList().contains(targetEntityType);
    }
}
