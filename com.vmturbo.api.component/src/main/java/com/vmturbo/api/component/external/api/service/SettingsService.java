package com.vmturbo.api.component.external.api.service;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingApiInputDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.ISettingsService;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SingleSettingSpecRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.GetAuditLogDataRetentionSettingRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetAuditLogDataRetentionSettingResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetStatsDataRetentionSettingsRequest;
import com.vmturbo.common.protobuf.stats.Stats.SetAuditLogDataRetentionSettingRequest;
import com.vmturbo.common.protobuf.stats.Stats.SetAuditLogDataRetentionSettingResponse;
import com.vmturbo.common.protobuf.stats.Stats.SetStatsDataRetentionSettingRequest;
import com.vmturbo.common.protobuf.stats.Stats.SetStatsDataRetentionSettingResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;

/**
 * Service implementation of Settings
 **/
public class SettingsService implements ISettingsService {

    private final SettingServiceBlockingStub settingServiceBlockingStub;

    private final SettingsMapper settingsMapper;

    private final SettingsManagerMapping settingsManagerMapping;

    private final StatsHistoryServiceBlockingStub statsServiceClient;

    public static final String PERSISTENCE_MANAGER = "persistencemanager";

    public SettingsService(@Nonnull final SettingServiceBlockingStub settingServiceBlockingStub,
                    @Nonnull final StatsHistoryServiceBlockingStub statsServiceClient,
                    @Nonnull final SettingsMapper settingsMapper,
                    @Nonnull final SettingsManagerMapping settingsManagerMapping) {

        this.settingServiceBlockingStub = settingServiceBlockingStub;
        this.statsServiceClient = Objects.requireNonNull(statsServiceClient);
        this.settingsMapper = settingsMapper;
        this.settingsManagerMapping = settingsManagerMapping;
    }

    @Override
    public List<SettingsManagerApiDTO> getSettings() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Retrieve settings by manager Uuid. A "manager" is a grouping for settings.
     *
     * @param uuid the manager uuid
     * @return a list of settings
     * @throws Exception
     */
    @Override
    public List<SettingApiDTO> getSettingsByUuid(String uuid) throws Exception {
        if (uuid.equals(PERSISTENCE_MANAGER)) {
            // These data retention settings don't go through the usual settings
            // service(group) framework as we rely on the sql db scheduled events to
            // do the purge of the expired data. These settings are stored in
            // vmtdb whose data ownership is handled by history/stats component.
            // In this case we are deviating from design "purity" for
            // efficiency purposes.
            List<SettingApiDTO> settingApiDtos = new LinkedList<>();
            statsServiceClient.getStatsDataRetentionSettings(
                GetStatsDataRetentionSettingsRequest.newBuilder().build())
                    .forEachRemaining(setting -> {
                        settingApiDtos.add(SettingsMapper.toSettingApiDto(setting));
                        });
            GetAuditLogDataRetentionSettingResponse response =
                statsServiceClient.getAuditLogDataRetentionSetting(
                    GetAuditLogDataRetentionSettingRequest.newBuilder().build());
            if (response.hasAuditLogRetentionSetting()) {
                settingApiDtos.add(SettingsMapper.toSettingApiDto(
                    response.getAuditLogRetentionSetting()));
            }
            return settingApiDtos;
        } else {
            SettingsManagerInfo managerInfo = settingsManagerMapping.getManagerInfo(uuid)
                    .orElseThrow(() -> new UnknownObjectException("Setting with Manager Uuid: "
                            + uuid + " is not found."));
            Iterable<Setting> settingIt = () -> settingServiceBlockingStub.getMultipleGlobalSettings(
                    GetMultipleGlobalSettingsRequest.newBuilder()
                            .addAllSettingSpecName(managerInfo.getSettings())
                            .build());
            return StreamSupport.stream(settingIt.spliterator(), false)
                    .map(SettingsMapper::toSettingApiDto)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public SettingApiDTO getSettingByUuidAndName(String uuid, String name) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Updates the value of a setting.
     *
     * @param uuid manager uuid
     * @param name Setting spec name
     * @param setting the setting value
     * @return the setting with the updated value
     * @throws Exception
     */
    @Override
    public SettingApiDTO putSettingByUuidAndName(String uuid, String name, SettingApiInputDTO setting) throws Exception {

        if (uuid.equals(PERSISTENCE_MANAGER)) {
            Optional<SettingApiDTO> newSetting;
            if (name.equals(GlobalSettingSpecs.AuditLogRetentionDays.getSettingName())) {
                newSetting = setAuditLogSettting(setting);
            } else {
                newSetting = setStatsRetentionSetting(name, setting);
            }

            return newSetting.orElseThrow(() -> new Exception("Failed to set the new setting value for " + name));

        } else {
            Objects.requireNonNull(name);
            Objects.requireNonNull(setting);

            SettingSpec spec = settingServiceBlockingStub.getSettingSpec(
                    SingleSettingSpecRequest.newBuilder()
                            .setSettingSpecName(name)
                            .build());
            if (spec != null) {
                UpdateGlobalSettingRequest.Builder updateRequestBuilder = UpdateGlobalSettingRequest.newBuilder()
                        .setSettingSpecName(name);
                switch (spec.getSettingValueTypeCase()) {
                    case BOOLEAN_SETTING_VALUE_TYPE:
                        if (!setting.getValue().equalsIgnoreCase(Boolean.TRUE.toString()) &&
                                !setting.getValue().equalsIgnoreCase(Boolean.FALSE.toString())) {
                            // Throw an exception with a more meaningful message if the boolean value is
                            // neither "true" nor "false" (case insensitive).
                            throw new IllegalArgumentException(
                                    String.format("Setting %s must have a boolean value. The value '%s' is invalid.",
                                            name, setting.getValue()));
                        }
                        updateRequestBuilder.setBooleanSettingValue(BooleanSettingValue.newBuilder()
                                .setValue(Boolean.valueOf(setting.getValue())));
                        break;
                    case NUMERIC_SETTING_VALUE_TYPE:
                        try {
                            updateRequestBuilder.setNumericSettingValue(NumericSettingValue.newBuilder()
                                    .setValue(Float.parseFloat(setting.getValue())));
                        } catch (NumberFormatException e) {
                            // Throw an exception with a more meaninful message if value is not a number.
                            throw new IllegalArgumentException(
                                    String.format("Setting %s must have a numeric value. The value '%s' is invalid. ",
                                            name, setting.getValue()));
                        }
                        break;
                    case ENUM_SETTING_VALUE_TYPE:
                        updateRequestBuilder.setEnumSettingValue(EnumSettingValue.newBuilder()
                                .setValue(setting.getValue()));
                        break;
                    case STRING_SETTING_VALUE_TYPE:
                        // fall through to next case
                    case SETTINGVALUETYPE_NOT_SET:
                        updateRequestBuilder.setStringSettingValue(StringSettingValue.newBuilder()
                                .setValue(setting.getValue()));
                        break;
                }

                settingServiceBlockingStub.updateGlobalSetting(updateRequestBuilder.build());
            } else {
                throw new IllegalArgumentException("Setting name is invalid: " + name);
            }

            Setting updatedSetting = settingServiceBlockingStub.getGlobalSetting(
                    GetSingleGlobalSettingRequest.newBuilder()
                            .setSettingSpecName(name)
                            .build());
            return SettingsMapper.toSettingApiDto(updatedSetting);
        }
    }

    private Optional<SettingApiDTO> setStatsRetentionSetting(String name, SettingApiInputDTO setting) {
        SetStatsDataRetentionSettingResponse response =
            statsServiceClient.setStatsDataRetentionSetting(
                SetStatsDataRetentionSettingRequest.newBuilder()
                    .setRetentionSettingName(name)
                    // The SettingSpec uses "float" for the setting numeric value
                    // type(NumericSettingDataType). So we are rounding to get an int
                    .setRetentionSettingValue(Math.round(Float.parseFloat(setting.getValue())))
                    .build());

        return (response.hasNewSetting() ?
                    Optional.of(SettingsMapper.toSettingApiDto(response.getNewSetting()))
                    : Optional.empty());
    }

    private Optional<SettingApiDTO> setAuditLogSettting(SettingApiInputDTO setting) {

        SetAuditLogDataRetentionSettingResponse response =
            statsServiceClient.setAuditLogDataRetentionSetting(
                SetAuditLogDataRetentionSettingRequest.newBuilder()
                    .setRetentionSettingValue(Math.round(Float.parseFloat(setting.getValue())))
                    .build());
        return (response.hasNewSetting() ?
                    Optional.of(SettingsMapper.toSettingApiDto(response.getNewSetting()))
                    : Optional.empty());

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
