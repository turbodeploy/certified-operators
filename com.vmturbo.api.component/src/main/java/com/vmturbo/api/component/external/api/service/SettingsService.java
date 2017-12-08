package com.vmturbo.api.component.external.api.service;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingApiInputDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.serviceinterfaces.ISettingsService;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.group.api.GlobalSettingSpecs;

/**
 * Service implementation of Settings
 **/
public class SettingsService implements ISettingsService {

    private final SettingServiceBlockingStub settingServiceBlockingStub;

    private final SettingsMapper settingsMapper;

    private final SettingsManagerMapping settingsManagerMapping;

    public SettingsService(@Nonnull final SettingServiceBlockingStub settingServiceBlockingStub,
                    @Nonnull final SettingsMapper settingsMapper,
                    @Nonnull final SettingsManagerMapping settingsManagerMapping) {
        this.settingServiceBlockingStub = settingServiceBlockingStub;
        this.settingsMapper = settingsMapper;
        this.settingsManagerMapping = settingsManagerMapping;
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
        final List<SettingsManagerApiDTO> retMgrs;

        // HACK for supporting RATE_OF_RESIZE in UI under Default VM Setting
        if ((entityType != null) &&
                (entityType.equals(ServiceEntityMapper.UIEntityType.VIRTUAL_MACHINE.getValue()))) {
            specs.add(GlobalSettingSpecs.RateOfResize.createSettingSpec());
        }

        if (managerUuid != null) {
            retMgrs = settingsMapper.toManagerDto(specs, managerUuid)
                    .map(Collections::singletonList)
                    .orElse(Collections.emptyList());
        } else {
            retMgrs = settingsMapper.toManagerDtos(specs);
        }

        // Set the entity type.
        //
        // At the time of this writing (Nov 2017) the UI is designed to only allow searching
        // settings by entity type. The API DTOs don't support multiple entity types per setting
        // spec, and there's no straightforward way to map a setting spec that supports multiple
        // entity types to an API setting spec. Our solution? Look up supported settings for that
        // entity type, and set the entity type of the SettingApiDTO to whatever the UI
        // requested.
        //
        // If it becomes necessary, we can create "fake" per-entity-type SettingApiDTOs when
        // no entity type is provided in the request. However, for now if the API/UI does not
        // request an entity type it will not be set in the returned SettingApiDTOs.
        if (entityType != null) {
            retMgrs.forEach(retMgr -> {
                if (retMgr.getSettings() != null) {
                    retMgr.getSettings().forEach(setting -> setting.setEntityType(entityType));
                }
            });
        }

        return isPlan ? settingsManagerMapping.convertToPlanSettingSpecs(retMgrs) : retMgrs;
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
