package com.vmturbo.api.component.external.api.service;

import java.io.Serializable;
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
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingApiDTOPossibilities;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.ISettingsService;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
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
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;

/**
 * Service implementation of Settings
 **/
public class SettingsService implements ISettingsService {

    private final SettingServiceBlockingStub settingServiceBlockingStub;

    private final SettingsMapper settingsMapper;

    private final SettingsManagerMapping settingsManagerMapping;

    private final StatsHistoryServiceBlockingStub statsServiceClient;

    private final SettingsPoliciesService settingsPoliciesService;

    /**
     * name of the manager for persistence.
     */
    public static final String PERSISTENCE_MANAGER = "persistencemanager";

    public SettingsService(@Nonnull final SettingServiceBlockingStub settingServiceBlockingStub,
                    @Nonnull final StatsHistoryServiceBlockingStub statsServiceClient,
                    @Nonnull final SettingsMapper settingsMapper,
                    @Nonnull final SettingsManagerMapping settingsManagerMapping,
                    @Nonnull final SettingsPoliciesService settingsPoliciesService) {
        this.settingServiceBlockingStub = settingServiceBlockingStub;
        this.statsServiceClient = Objects.requireNonNull(statsServiceClient);
        this.settingsMapper = settingsMapper;
        this.settingsManagerMapping = settingsManagerMapping;
        this.settingsPoliciesService = settingsPoliciesService;
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
    public List<? extends SettingApiDTO<?>> getSettingsByUuid(String uuid) throws Exception {
        if (uuid.equals(PERSISTENCE_MANAGER)) {
            // These data retention settings don't go through the usual settings
            // service(group) framework as we rely on the sql db scheduled events to
            // do the purge of the expired data. These settings are stored in
            // vmtdb whose data ownership is handled by history/stats component.
            // In this case we are deviating from design "purity" for
            // efficiency purposes.
            List<SettingApiDTO<?>> settingApiDtos = new LinkedList<>();
            statsServiceClient.getStatsDataRetentionSettings(
                GetStatsDataRetentionSettingsRequest.newBuilder().build())
                    .forEachRemaining(setting -> settingsMapper.toSettingApiDto(setting)
                            .getGlobalSetting().ifPresent(settingApiDtos::add));
            GetAuditLogDataRetentionSettingResponse response =
                statsServiceClient.getAuditLogDataRetentionSetting(
                    GetAuditLogDataRetentionSettingRequest.newBuilder().build());
            if (response.hasAuditLogRetentionSetting()) {
                settingsMapper.toSettingApiDto(response.getAuditLogRetentionSetting())
                        .getGlobalSetting().ifPresent(settingApiDtos::add);
            }
            //noinspection unchecked
            return (List<SettingApiDTO<?>>)(List<?>) settingApiDtos;
        } else {
            // check if the input manager is supported
            final SettingsManagerInfo managerInfo = settingsManagerMapping.getManagerInfo(uuid)
                    .orElseThrow(() -> new UnknownObjectException("Setting with Manager Uuid: "
                            + uuid + " is not found."));
            // go through all default policies and collect all the settings for the given manager
            List<SettingApiDTO<?>> settingApiDTOs = settingsPoliciesService.getSettingsPolicies(
                    true, Collections.emptySet(), Sets.newHashSet(uuid)).stream()
                    .flatMap(sp -> CollectionUtils.emptyIfNull(sp.getSettingsManagers()).stream())
                    .filter(manager -> StringUtils.equals(uuid, manager.getUuid()))
                    .flatMap(manager -> CollectionUtils.emptyIfNull(manager.getSettings()).stream())
                    .collect(Collectors.toList());
            // if no settings found from default policies, then try to find from global settings
            // (e.g. reservedintancemanager settings, emailmanager settings...) which are not
            // associated with any entity type
            if (settingApiDTOs.isEmpty()) {
                Iterable<Setting> settingIt = () -> settingServiceBlockingStub.getMultipleGlobalSettings(
                        GetMultipleGlobalSettingsRequest.newBuilder()
                                .addAllSettingSpecName(managerInfo.getSettings())
                                .build());
                settingApiDTOs = StreamSupport.stream(settingIt.spliterator(), false)
                        .map(settingsMapper::toSettingApiDto)
                        .map(SettingApiDTOPossibilities::getGlobalSetting)
                        .filter(Optional::isPresent).map(Optional::get)
                        .collect(Collectors.toList());
            }
            return settingApiDTOs;
        }
    }

    @Override
    public <T extends Serializable> SettingApiDTO<T> getSettingByUuidAndName(String uuid, String name) throws Exception {
        // this api should only be used for global settings which are not associated with any
        // entity types (like: reservedintancemanager settings, emailmanager settings...), or is
        // only associated with one type of entity (like marketsettingsmanager.targetBand).
        // if it's used for entity settings which may apply to multiple entity types, there will
        // be multiple settings for different entity types (like: transactionsCapacity), in which
        // case the SettingsPolicies api should be used.
        return (SettingApiDTO<T>)getSettingsByUuid(uuid).stream()
                .filter(setting -> StringUtils.equals(name, setting.getUuid()))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Setting with manager uuid: "
                        + uuid + " and name: " + name + " is not found"));
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
    public <T extends Serializable> SettingApiDTO<T> putSettingByUuidAndName(String uuid, String name, SettingApiDTO<T> setting) throws Exception {

        String settingValue = StringUtils.trimToEmpty(SettingsMapper.inputValueToString(setting).orElse(""));

        if (uuid.equals(PERSISTENCE_MANAGER)) {
            Optional<SettingApiDTO<String>> newSetting;
            if (name.equals(GlobalSettingSpecs.AuditLogRetentionDays.getSettingName())) {
                newSetting = setAuditLogSettting(settingValue);
            } else {
                newSetting = setStatsRetentionSetting(name, settingValue);
            }

            //noinspection unchecked
            return (SettingApiDTO<T>) newSetting.orElseThrow(() -> new Exception("Failed to set the new setting value for " + name));

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
                        if (!StringUtils.equalsIgnoreCase(settingValue, Boolean.TRUE.toString()) &&
                                !StringUtils.equalsIgnoreCase(settingValue, Boolean.FALSE.toString())) {
                            // Throw an exception with a more meaningful message if the boolean value is
                            // neither "true" nor "false" (case insensitive).
                            throw new IllegalArgumentException(
                                    String.format("Setting %s must have a boolean value. The value '%s' is invalid.",
                                            name, settingValue));
                        }
                        updateRequestBuilder.setBooleanSettingValue(BooleanSettingValue.newBuilder()
                                .setValue(Boolean.valueOf(settingValue)));
                        break;
                    case NUMERIC_SETTING_VALUE_TYPE:
                        try {
                            updateRequestBuilder.setNumericSettingValue(NumericSettingValue.newBuilder()
                                    .setValue(Float.parseFloat(settingValue)));
                        } catch (NumberFormatException e) {
                            // Throw an exception with a more meaninful message if value is not a number.
                            throw new IllegalArgumentException(
                                    String.format("Setting %s must have a numeric value. The value '%s' is invalid. ",
                                            name, settingValue));
                        }
                        break;
                    case ENUM_SETTING_VALUE_TYPE:
                        updateRequestBuilder.setEnumSettingValue(EnumSettingValue.newBuilder()
                                .setValue(settingValue));
                        break;
                    case STRING_SETTING_VALUE_TYPE:
                        // fall through to next case
                    case SETTINGVALUETYPE_NOT_SET:
                        updateRequestBuilder.setStringSettingValue(StringSettingValue.newBuilder()
                                .setValue(settingValue));
                        break;
                }

                settingServiceBlockingStub.updateGlobalSetting(updateRequestBuilder.build());
            } else {
                throw new IllegalArgumentException("Setting name is invalid: " + name);
            }

            final GetGlobalSettingResponse response = settingServiceBlockingStub.getGlobalSetting(
                    GetSingleGlobalSettingRequest.newBuilder()
                            .setSettingSpecName(name)
                            .build());
            if (response.hasSetting()) {
                SettingApiDTO<String> stringSettingApiDTO = settingsMapper.toSettingApiDto(response.getSetting()).getGlobalSetting()
                        .orElseThrow(() -> new IllegalStateException("No global setting parsed from " +
                                "global setting response"));
                //noinspection unchecked
                return (SettingApiDTO<T>) stringSettingApiDTO;
            } else {
                throw new UnknownObjectException("Unknown setting: " + name);
            }
        }
    }

    private Optional<SettingApiDTO<String>> setStatsRetentionSetting(String name, final String settingValue) {
        final SetStatsDataRetentionSettingResponse response =
            statsServiceClient.setStatsDataRetentionSetting(
                SetStatsDataRetentionSettingRequest.newBuilder()
                    .setRetentionSettingName(name)
                    // The SettingSpec uses "float" for the setting numeric value
                    // type(NumericSettingDataType). So we are rounding to get an int
                    .setRetentionSettingValue(Math.round(Float.parseFloat(settingValue)))
                    .build());

        return response.hasNewSetting() ?
                settingsMapper.toSettingApiDto(response.getNewSetting()).getGlobalSetting() :
                Optional.empty();
    }

    private Optional<SettingApiDTO<String>> setAuditLogSettting(final String settingValue) {
        final SetAuditLogDataRetentionSettingResponse response =
            statsServiceClient.setAuditLogDataRetentionSetting(
                SetAuditLogDataRetentionSettingRequest.newBuilder()
                    .setRetentionSettingValue(Math.round(Float.parseFloat(settingValue)))
                    .build());
        return response.hasNewSetting() ?
                settingsMapper.toSettingApiDto(response.getNewSetting()).getGlobalSetting() :
                Optional.empty();
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

        if (managerUuid != null) {
            retMgrs = settingsMapper.toManagerDto(specs, Optional.ofNullable(entityType), managerUuid)
                    .map(Collections::singletonList)
                    .orElse(Collections.emptyList());
        } else {
            retMgrs = settingsMapper.toManagerDtos(specs, Optional.ofNullable(entityType));
        }

        return isPlan ? settingsManagerMapping.convertToPlanSettingSpecs(retMgrs) : retMgrs;
    }

    @VisibleForTesting
    static boolean settingMatchEntityType(@Nonnull final SettingSpec settingSpec,
                                          @Nullable String entityType) {
        if (entityType == null) {
            return true;
        }

        if (settingSpec.hasGlobalSettingSpec()) {
            return StringUtils.equals(entityType,
                SettingsMapper.GLOBAL_SETTING_ENTITY_TYPES.get(settingSpec.getName()));
        }

        if (!settingSpec.hasEntitySettingSpec() ||
            !settingSpec.getEntitySettingSpec().hasEntitySettingScope()) {
            return false;
        }

        final int targetEntityType = UIEntityType.fromString(entityType).typeNumber();
        EntitySettingScope scope = settingSpec.getEntitySettingSpec().getEntitySettingScope();

        return scope.hasAllEntityType() ||
            scope.getEntityTypeSet().getEntityTypeList().contains(targetEntityType);
    }

    /**
     * get a setting by entity or group uuid, settings manager name and setting name
     * GET /entities/{uuid}/settings/{manager_uuid}/{setting_uuid}/
     *
     * @param uuid     of entity or group
     * @param manUuid  name of settings manager
     * @param settUuid name of setting
     * @return SettingApiDTO
     * @throws Exception
     */
    public <T extends Serializable> SettingApiDTO<T> getSettingByGroupOrEntity(String uuid, String manUuid,
                                            String settUuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * get list of settings by entity or group uuid and settings manager name
     * GET /entities/{uuid}/settings/{manager_uuid}/
     *
     * @param uuid    of entity or group
     * @param manUuid name of settings manager
     * @return list of SettingApiDTO
     * @throws Exception
     */
    public List<? extends SettingApiDTO<?>> getSettingsManagerByGroupOrEntity(String uuid, String manUuid
    ) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }
}
