package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.mapper.SettingsMapper.SETTING_ENTITY_TYPE_MAP;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
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
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.ISettingsService;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
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
import com.vmturbo.common.protobuf.stats.Stats.SetAuditLogDataRetentionSettingRequest;
import com.vmturbo.common.protobuf.stats.Stats.SetAuditLogDataRetentionSettingResponse;
import com.vmturbo.common.protobuf.stats.Stats.SetStatsDataRetentionSettingRequest;
import com.vmturbo.common.protobuf.stats.Stats.SetStatsDataRetentionSettingResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;

/**
 * Service implementation of Settings.
 **/
public class SettingsService implements ISettingsService {

    private final SettingServiceBlockingStub settingServiceBlockingStub;

    private final SettingsMapper settingsMapper;

    private final SettingsManagerMapping settingsManagerMapping;

    private final StatsHistoryServiceBlockingStub statsServiceClient;

    private final SettingsPoliciesService settingsPoliciesService;

    private static final Set<String> STATS_NON_AUDIT_RETENTION_SETTINGS = ImmutableSet.of(
            GlobalSettingSpecs.StatsRetentionHours.getSettingName(),
            GlobalSettingSpecs.StatsRetentionDays.getSettingName(),
            GlobalSettingSpecs.StatsRetentionMonths.getSettingName()
    );

    /**
     * name of the manager for persistence.
     */
    public static final String PERSISTENCE_MANAGER = "persistencemanager";

    /**
     * name of the manager for automation.
     */
    public static final String AUTOMATION_MANAGER = "automationmanager";

    /**
     * The name of the manager for action workflow.
     */
    public static final String CONTROL_MANAGER = "controlmanager";

    /**
     * The name of the manager for market settings.
     */
    public static final String MARKETSETTINGS_MANAGER = "marketsettingsmanager";

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
     * @throws UnknownObjectException when a setting manger is not found by the uuid specified
     */
    @Override
    public List<? extends SettingApiDTO<?>> getSettingsByUuid(String uuid) throws UnknownObjectException {
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

    @Override
    public <T extends Serializable> SettingApiDTO<T> getSettingByUuidAndName(String uuid, String name) throws Exception {
        // this api should only be used for global settings which are not associated with any
        // entity types (like: reservedintancemanager settings, emailmanager settings...), or is
        // only associated with one type of entity (like marketsettingsmanager.targetBand).
        // if it's used for entity settings which may apply to multiple entity types, there will
        // be multiple settings for different entity types (like: transactionSLO), in which
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
     * @param uuid    manager uuid
     * @param name    Setting spec name
     * @param setting the setting value
     * @return the setting with the updated value
     * @throws Exception
     */
    @Override
    public <T extends Serializable> SettingApiDTO<T> putSettingByUuidAndName(
            String uuid,
            @Nonnull String name,
            @Nonnull SettingApiDTO<T> setting) throws Exception {

        Objects.requireNonNull(name);
        Objects.requireNonNull(setting);

        String settingValue = StringUtils.trimToEmpty(SettingsMapper.inputValueToString(setting).orElse(""));

        if (uuid.equals(PERSISTENCE_MANAGER)) {
            // These data retention settings need to be persisted to the history database,
            // in addition to updating the settings store. So before sending to
            // the group setting service, we first attempt the DB update, and then
            // if that succeeds, we proceed with normal settings update.
            Optional<SettingApiDTO<String>> newSetting = null;
            if (name.equals(GlobalSettingSpecs.AuditLogRetentionDays.getSettingName())) {
                newSetting = setAuditLogSettting(settingValue);
            } else if (STATS_NON_AUDIT_RETENTION_SETTINGS.contains(name)) {
                newSetting = setStatsRetentionSetting(name, settingValue);
            }
            // null means there was no DB settings update required for this setting
            if (newSetting != null && !newSetting.isPresent()) {
                throw new Exception("Failed to set the new setting value for " + name);
            }
        }
        SettingSpec spec = settingServiceBlockingStub.getSettingSpec(
                SingleSettingSpecRequest.newBuilder()
                        .setSettingSpecName(name)
                        .build());
        if (spec == null) {
            throw new IllegalArgumentException("Setting name is invalid: " + name);
        }
        if (spec.hasGlobalSettingSpec()) {
            return putGlobalSettingByUuidAndName(name, spec, settingValue);
        } else {
            // If it's not a global setting, it's a setting for entity defaults.
            return putEntityDefaultSettingByUuidAndName(name, spec, settingValue, setting, uuid);
        }
    }

    /**
     * Updates a global setting.
     *
     * @param <T> The type of the dto
     * @param name the setting's name
     * @param spec the setting spec describing the setting to update
     * @param settingValue the new value to update the setting with
     *
     * @return A SettingApiDTO describing the updated setting.
     *
     * @throws UnknownObjectException If the specified setting could not be found.
     */
    private <T extends Serializable> SettingApiDTO<T> putGlobalSettingByUuidAndName(
            @Nonnull String name,
            @Nonnull SettingSpec spec,
            String settingValue
    ) throws UnknownObjectException {
        Setting.Builder settingBuilder = Setting.newBuilder().setSettingSpecName(name);

        SettingsMapper.validateSettingValue(settingValue, spec);
        switch (spec.getSettingValueTypeCase()) {
            case BOOLEAN_SETTING_VALUE_TYPE:
                settingBuilder.setBooleanSettingValue(
                        BooleanSettingValue.newBuilder().setValue(Boolean.valueOf(settingValue)));
                break;
            case NUMERIC_SETTING_VALUE_TYPE:
                settingBuilder.setNumericSettingValue(NumericSettingValue.newBuilder()
                        .setValue(Float.parseFloat(settingValue)));
                break;
            case ENUM_SETTING_VALUE_TYPE:
                settingBuilder.setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue(ActionDTOUtil.mixedSpacesToUpperUnderScore(settingValue)));
                break;
            case STRING_SETTING_VALUE_TYPE:
                // fall through to next case
            case SETTINGVALUETYPE_NOT_SET:
                settingBuilder.setStringSettingValue(
                        StringSettingValue.newBuilder().setValue(settingValue));
                break;
        }

        settingServiceBlockingStub.updateGlobalSetting(
                UpdateGlobalSettingRequest.newBuilder().addSetting(settingBuilder).build());

        final GetGlobalSettingResponse response = settingServiceBlockingStub.getGlobalSetting(
                GetSingleGlobalSettingRequest.newBuilder().setSettingSpecName(name).build());
        if (response.hasSetting()) {
            SettingApiDTO<String> stringSettingApiDTO = settingsMapper.toSettingApiDto(
                            response.getSetting())
                    .getGlobalSetting()
                    .orElseThrow(() -> new IllegalStateException("No global setting parsed from "
                            + "global setting response"));
            //noinspection unchecked
            return (SettingApiDTO<T>)stringSettingApiDTO;
        } else {
            throw new UnknownObjectException("Unknown setting: " + name);
        }
    }

    /**
     * Updates an entity default setting.
     *
     * @param <T> The type of the dto
     * @param name the setting's name
     * @param spec the setting spec describing the setting to update
     * @param settingValue the new value to update the setting with
     * @param setting the original SettingApiDTO provided as input
     * @param settingsManagerUuid the uuid of the settingsManager specified
     *
     * @return A SettingApiDTO describing the updated setting.
     *
     * @throws Exception UnknownObjectException if no setting with values provided can be found, or
     *                  general RuntimeException if an error occurs during conversions.
     */
    private <T extends Serializable> SettingApiDTO<T> putEntityDefaultSettingByUuidAndName(
            @Nonnull String name,
            @Nonnull SettingSpec spec,
            String settingValue,
            @Nonnull SettingApiDTO<T> setting,
            String settingsManagerUuid
    ) throws Exception {
        ApiEntityType settingEntityType = ApiEntityType.fromString(setting.getEntityType());
        validateInputForEntityDefaultSetting(name, setting, settingEntityType);
        // get the appropriate settings policy & settings manager
        List<SettingsPolicyApiDTO> settingsPolicies = settingsPoliciesService.getSettingsPolicies(
                true,
                Collections.singleton(settingEntityType.typeNumber()),
                Collections.singleton(settingsManagerUuid));
        if (settingsPolicies.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot find default settings policy for entity type '" + settingEntityType
                    + "'.");
        } else if (settingsPolicies.size() > 1) {
            throw new OperationFailedException("Failed to uniquely identify default policy for"
                    + " entity type '" + setting.getEntityType() + "'. Found "
                    + settingsPolicies.size() + " policies.");
        } else if (settingsPolicies.get(0).getSettingsManagers().isEmpty()) {
            throw new IllegalArgumentException("Cannot find settings manager '"
                    + settingsManagerUuid + "'.");
        }
        SettingsPolicyApiDTO settingsPolicyApiDTO = settingsPolicies.get(0);
        SettingsManagerApiDTO settingsManager = settingsPolicyApiDTO.getSettingsManagers().get(0);
        // populate the dto with the updated values
        SettingApiDTO settingDtoWithUpdatedValues = settingsManager
                .getSettings()
                .stream()
                .filter(s -> s.getUuid().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Could not retrieve setting with uuid '" + name
                        + "' under settings manager '" + settingsManager.getUuid() + "'."));
        settingDtoWithUpdatedValues.setValue(settingValue);
        List<SettingApiDTO<?>> settingsToUpdate = new ArrayList<>();
        settingsToUpdate.add(settingDtoWithUpdatedValues);
        settingsManager.setSettings(settingsToUpdate);
        // send the request to update the query using the updated dto
        SettingsPolicyApiDTO response =
                settingsPoliciesService.editSettingsPolicy(settingsPolicyApiDTO.getUuid(), false,
                        settingsPolicyApiDTO);
        return (SettingApiDTO<T>)response.getSettingsManagers()
                .stream()
                .filter(settingsManagerApiDTO ->
                        settingsManagerApiDTO.getUuid().equals(settingsManagerUuid))
                .findFirst()
                .orElseThrow(() -> new OperationFailedException(
                        "Failed to retrieve settings manager after update."))
                .getSettings()
                .stream()
                .filter(settingApiDTO -> settingApiDTO.getUuid().equals(name))
                .findFirst()
                .orElseThrow(() -> new OperationFailedException(
                        "Failed to retrieve setting after update."));
    }

    /**
     * Validates that the input is correct when updating an entity default setting.
     *
     * @param <T> The type of the dto
     * @param name the setting's name
     * @param setting the original SettingApiDTO provided as input
     * @param entityType the entity type of the setting to update
     */
    private <T extends Serializable> void validateInputForEntityDefaultSetting(
            @Nonnull String name,
            @Nonnull SettingApiDTO<T> setting,
            ApiEntityType entityType
    ) {
        // Make sure that the uuid field is populated.
        if (setting.getUuid() == null || setting.getUuid().isEmpty()) {
            setting.setUuid(name);
        } else if (!setting.getUuid().equals(name)) {
            // some sanity check in case setting name is provided in both URI and input dto with
            // different values
            throw new IllegalArgumentException("Multiple uuids provided instead of one: '"
                    + name + "', '" + setting.getUuid() + "'");
        }
        // make sure that entity type is provided (to be able to pick the correct default policy
        // in case this setting applies to multiple entity types)
        if (entityType == ApiEntityType.UNKNOWN) {
            throw new IllegalArgumentException(
                    "Missing or incorrect entity type for setting " + "with uuid: "
                            + setting.getUuid()
                            + ". (entity type is required for entity settings)");
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
                                                        final boolean isPlan) {
        final Iterable<SettingSpec> specIt = () -> settingServiceBlockingStub.searchSettingSpecs(
                SearchSettingSpecsRequest.getDefaultInstance());

        final List<SettingSpec> specs = StreamSupport.stream(specIt.spliterator(), false)
                .filter(spec -> settingMatchEntityType(spec, entityType))
                .collect(Collectors.toList());
        final List<SettingsManagerApiDTO> retMgrs;

        if (managerUuid != null) {
            retMgrs = settingsMapper.toManagerDto(specs, Optional.ofNullable(entityType),
                managerUuid, isPlan)
                    .map(Collections::singletonList)
                    .orElse(Collections.emptyList());
        } else {
            retMgrs = settingsMapper.toManagerDtos(specs, Optional.ofNullable(entityType), isPlan);
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

        final int targetEntityType = ApiEntityType.fromString(entityType).typeNumber();
        EntitySettingScope scope = settingSpec.getEntitySettingSpec().getEntitySettingScope();

        return scope.hasAllEntityType()
            || scope.getEntityTypeSet().getEntityTypeList().contains(targetEntityType)
            || hasMappedEntityType(scope, entityType);
    }

    /**
     * Check if entity setting scope has corresponding mapped entity type from given entity type.
     * Some settings of one entity type are configured on a different type, for example container
     * resize setting is available for container spec entity type because container is ephemeral,
     * while container spec is persistent. This method is to ensure correct settings can be returned
     * for the given entity type.
     *
     * @param scope      Given {@link EntitySettingScope}.
     * @param entityType Given entity type to be mapped from.
     * @return True if entity setting scope contains mapped entity type from given entity type.
     */
    private static boolean hasMappedEntityType(@Nonnull final EntitySettingScope scope,
                                               @Nonnull final String entityType) {
        String mappedEntityType = SETTING_ENTITY_TYPE_MAP.get(entityType);
        return mappedEntityType != null
            && scope.getEntityTypeSet().getEntityTypeList()
            .contains(ApiEntityType.fromString(mappedEntityType).typeNumber());
    }
}
