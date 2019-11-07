package com.vmturbo.api.component.external.api.mapper;

import java.io.Serializable;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.api.component.external.api.mapper.SettingSpecStyleMappingLoader.SettingSpecStyleMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingOptionApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.settingspolicy.RecurrenceApiDTO;
import com.vmturbo.api.dto.settingspolicy.ScheduleApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.enums.DayOfWeek;
import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.api.enums.RecurrenceType;
import com.vmturbo.api.enums.SettingScope;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting.ValueCase;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.SettingValueTypeCase;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.OsMigrationSettingsEnum.OperatingSystem;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Responsible for mapping Settings-related XL objects to their API counterparts.
 */
public class SettingsMapper {

    private static final Logger logger = LogManager.getLogger();

    /**
     * This is used to inject the value of the {@link SettingApiDTO} into the corresponding
     * {@link Setting} according to the value type of the {@link SettingSpec}.
     */
    private static final Map<SettingValueTypeCase, ProtoSettingValueInjector> PROTO_SETTING_VALUE_INJECTORS =
        ImmutableMap.<SettingValueTypeCase, ProtoSettingValueInjector>builder()
            .put(SettingValueTypeCase.BOOLEAN_SETTING_VALUE_TYPE,
                (val, builder) -> builder.setBooleanSettingValue(BooleanSettingValue.newBuilder()
                    .setValue(Boolean.valueOf(val))))
            .put(SettingValueTypeCase.NUMERIC_SETTING_VALUE_TYPE,
                (val, builder) -> builder.setNumericSettingValue(NumericSettingValue.newBuilder()
                    .setValue(Float.valueOf(val))))
            .put(SettingValueTypeCase.STRING_SETTING_VALUE_TYPE,
                (val, builder) -> builder.setStringSettingValue(StringSettingValue.newBuilder()
                .setValue(val)))
            .put(SettingValueTypeCase.ENUM_SETTING_VALUE_TYPE,
                (val, builder) -> builder.setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue(ActionDTOUtil.mixedSpacesToUpperUnderScore(val))))
            .put(SettingValueTypeCase.SORTED_SET_OF_OID_SETTING_VALUE_TYPE,
                // Here we sort strictly the list of oids provided by UI/API.
                (val, builder) -> builder.setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue
                    .newBuilder().addAllOids(() -> Arrays.stream(StringUtils.split(val, ","))
                        .mapToLong(Long::valueOf).sorted().distinct().iterator())))
            .build();

    /**
     * This is used to inject the value of the {@link Setting} into the corresponding
     * {@link SettingApiDTO} according to the value type of the {@link Setting}.
     * This can be used to create default settings, such as Virtual Machine Defaults.
     */
    private static final Map<ValueCase, ApiSettingValueInjector> API_SETTING_VALUE_INJECTORS =
        ImmutableMap.<ValueCase, ApiSettingValueInjector>builder()
            .put(ValueCase.BOOLEAN_SETTING_VALUE, (setting, apiDTO) -> {
                apiDTO.setValue(Boolean.toString(setting.getBooleanSettingValue().getValue()));
                apiDTO.setValueType(InputValueType.BOOLEAN);
            })
            .put(ValueCase.NUMERIC_SETTING_VALUE, (setting, apiDTO) -> {
                apiDTO.setValue(Float.toString(setting.getNumericSettingValue().getValue()));
                apiDTO.setValueType(InputValueType.NUMERIC);
            })
            .put(ValueCase.STRING_SETTING_VALUE, (setting, apiDTO) -> {
                apiDTO.setValue(setting.getStringSettingValue().getValue());
                apiDTO.setValueType(InputValueType.STRING);
            })
            .put(ValueCase.ENUM_SETTING_VALUE, (setting, apiDTO) -> {
                apiDTO.setValue(setting.getEnumSettingValue().getValue());
                apiDTO.setValueType(InputValueType.STRING);
            })
            .put(ValueCase.SORTED_SET_OF_OID_SETTING_VALUE, (setting, apiDTO) -> {
                apiDTO.setValue(setting.getSortedSetOfOidSettingValue().getOidsList().stream()
                    .map(String::valueOf).collect(Collectors.joining(",")));
                apiDTO.setValueType(InputValueType.LIST);
            })
            .build();

    public static final String GLOBAL_ACTION_SETTING_NAME = "Global Action Mode Defaults";

    public static final int GLOBAL_ACTION_SETTING_NAME_ID = 55555;

    /**
     * A constant indicating that a {@link SettingSpec} has no associated setting manager.
     */
    private static final String NO_MANAGER = "";

    private static final String SERVICE_ENTITY = "ServiceEntity";

    private final SettingsManagerMapping managerMapping;

    private final SettingSpecStyleMapping settingSpecStyleMapping;

    private final SettingSpecMapper settingSpecMapper;

    private final SettingPolicyMapper settingPolicyMapper;

    private final GroupServiceBlockingStub groupService;

    private final SettingServiceBlockingStub settingService;

    private final SettingPolicyServiceBlockingStub settingPolicyService;

    /**
     * (April 10 2017) In the UI, all settings - including global settings - are organized by
     * entity type. This map contains the "global setting name" -> "entity type" mappings to use
     * when formatting DTOs to return to the UI.
     */
    public static final Map<String, String> GLOBAL_SETTING_ENTITY_TYPES =
        ImmutableMap.<String, String>builder()
            .put(GlobalSettingSpecs.RateOfResize.getSettingName(), UIEntityType.VIRTUAL_MACHINE.apiStr())
            .put(GlobalSettingSpecs.DisableAllActions.getSettingName(), SERVICE_ENTITY)
            .build();

    /**
     * map to get the predefined label for a given setting enum. if not found, we will use
     * {@link com.vmturbo.common.protobuf.action.ActionDTOUtil#upperUnderScoreToMixedSpaces} to
     * convert the enum to a beautiful string. we want to show "RHEL" rather than "Rhel" in UI.
     */
    private static final Map<String, String> SETTING_ENUM_NAME_TO_LABEL = ImmutableMap.of(
            OperatingSystem.RHEL.name(), "RHEL",
            OperatingSystem.SLES.name(), "SLES"
    );

    public SettingsMapper(@Nonnull final SettingServiceBlockingStub settingService,
                          @Nonnull final GroupServiceBlockingStub groupService,
                          @Nonnull final SettingPolicyServiceBlockingStub settingPolicyService,
                          @Nonnull final SettingsManagerMapping settingsManagerMapping,
                          @Nonnull final SettingSpecStyleMapping settingSpecStyleMapping) {
        this.managerMapping = settingsManagerMapping;
        this.settingSpecStyleMapping = settingSpecStyleMapping;
        this.settingSpecMapper = new DefaultSettingSpecMapper();
        this.groupService = groupService;
        this.settingService = settingService;
        this.settingPolicyService = settingPolicyService;
        this.settingPolicyMapper = new DefaultSettingPolicyMapper(this, settingService);
    }

    @VisibleForTesting
    SettingsMapper(@Nonnull final SettingsManagerMapping managerMapping,
                   @Nonnull final SettingSpecStyleMapping settingSpecStyleMapping,
                   @Nonnull final SettingSpecMapper specMapper,
                   @Nonnull final SettingPolicyMapper policyMapper,
                   @Nonnull final Channel groupComponentChannel) {
        this.managerMapping = managerMapping;
        this.settingSpecStyleMapping = settingSpecStyleMapping;
        this.settingSpecMapper = specMapper;
        this.settingPolicyMapper = policyMapper;
        this.groupService = GroupServiceGrpc.newBlockingStub(groupComponentChannel);
        this.settingService = SettingServiceGrpc.newBlockingStub(groupComponentChannel);
        this.settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(groupComponentChannel);
    }

    /**
     * Get the entity type associated with a new {@link SettingsPolicyApiDTO} received through
     * the API (usually from the UI) for setting policy creation. Since the UI input does not
     * always set the entity type directly, we infer it from the scopes of the policy.
     *
     * @param settingPolicy The setting policy DTO received from the API/UI to create a setting
     *                      policy.
     * @return The entity type that should be associated with the setting policy.
     * @throws InvalidOperationException If the input is illegal.
     * @throws UnknownObjectException If the groups the setting policy is scoped to don't exist.
     */
    @VisibleForTesting
    int resolveEntityType(@Nonnull final SettingsPolicyApiDTO settingPolicy)
            throws InvalidOperationException, UnknownObjectException {
        if (settingPolicy.getScopes() == null || settingPolicy.getScopes().isEmpty()) {
            throw new InvalidOperationException("Unscoped setting policy: " +
                    settingPolicy.getDisplayName());
        }

        final Set<Long> groupIds = settingPolicy.getScopes().stream()
                .filter(group -> group.getUuid() != null)
                .map(GroupApiDTO::getUuid)
                .filter(Objects::nonNull)
                .map(Long::valueOf)
                .collect(Collectors.toSet());
        if (groupIds.isEmpty()) {
            throw new InvalidOperationException("No IDS specified for scopes in setting policy: " +
                    settingPolicy.getDisplayName());
        }

        // Get all groups referred to by the scopes of the input policy.
        final Iterable<Grouping> groups = () -> groupService.getGroups(
                        GetGroupsRequest.newBuilder()
                            .setGroupFilter(GroupFilter.newBuilder()
                                            .addAllId(groupIds))
                            .build());

        final Map<Integer, List<Grouping>> groupsByEntityType = new HashMap<>();

        StreamSupport.stream(groups.spliterator(), false)
            .forEach(group -> {
                GroupProtoUtil.getEntityTypes(group)
                    .forEach(type -> groupsByEntityType.computeIfAbsent(type.typeNumber(),
                            ArrayList::new).add(group));
            });

        if (groupsByEntityType.isEmpty()) {
            // This can only happen if the groups are not found.
            throw new UnknownObjectException("Group IDs " + groupIds + " not found.");
        } else if (groupsByEntityType.size() > 1) {
            // All the groups should share the same entity type.
            throw new InvalidOperationException("Setting policy scopes have " +
                "different entity types: " + groupsByEntityType.keySet().stream()
                    .map(EntityType::forNumber)
                    .map(EntityType::name)
                    .collect(Collectors.joining(",")));
        } else {
            return groupsByEntityType.keySet().iterator().next();
        }
    }

    /**
     * Convert a collection of {@link SettingSpec} objects into the {@link SettingsManagerApiDTO}
     * that will represent these settings to the UI.
     *
     * @param specs The collection of specs.
     * @param entityType An {@link Optional} containing the entity type to limit returned
     *                   {@link SettingApiDTO}s to. This is necessary because a {@link SettingSpec}
     *                   may be associated with multiple entity types.
     * @return The {@link SettingsManagerApiDTO}s.
     */
    public List<SettingsManagerApiDTO> toManagerDtos(@Nonnull final Collection<SettingSpec> specs,
                                                     @Nonnull final Optional<String> entityType) {
        final Map<String, List<SettingSpec>> specsByMgr = specs.stream()
                .collect(Collectors.groupingBy(spec -> managerMapping.getManagerUuid(spec.getName())
                        .orElse(NO_MANAGER)));

        final List<SettingSpec> unhandledSpecs = specsByMgr.get(NO_MANAGER);
        if (unhandledSpecs != null && !unhandledSpecs.isEmpty()) {
            final List<String> unhandledNames = unhandledSpecs.stream()
                    .map(SettingSpec::getName)
                    .collect(Collectors.toList());
            logger.warn("The following settings don't have a mapping in the API component." +
                    " Not returning them to the user. Settings: {}", unhandledNames);
        }

        return specsByMgr.entrySet().stream()
                // Don't return the specs that don't have a Manager mapping
                .filter(entry -> !entry.getKey().equals(NO_MANAGER))
                .map(entry -> {
                    final SettingsManagerInfo info = managerMapping.getManagerInfo(entry.getKey())
                            .orElseThrow(() -> new IllegalStateException("Manager ID " +
                                    entry.getKey() + " not found despite being in the mappings earlier."));
                    return createMgrDto(entry.getKey(), entityType, info,
                        info.sortSettingSpecs(entry.getValue(), SettingSpec::getName));
                })
                .collect(Collectors.toList());
    }

    /**
     * Convert a collection of {@link SettingSpec} objects into a specific
     * {@link SettingsManagerApiDTO}. This is like {@link SettingsMapper#toManagerDtos(Collection, Optional)},
     * just for one specific manager.
     *
     * @param specs The {@link SettingSpec}s. These don't have to all be managed by the desired
     *              manager - the method will exclude the ones that are not.
     * @param desiredMgrId The ID of the desired manager ID.
     * @return An optional containing the {@link SettingsManagerApiDTO}, or an empty optional if
     *         the manager does not exist in the mappings.
     */
    public Optional<SettingsManagerApiDTO> toManagerDto(
            @Nonnull final Collection<SettingSpec> specs,
            @Nonnull final Optional<String> entityType,
            @Nonnull final String desiredMgrId) {
        final Optional<SettingsManagerInfo> infoOpt = managerMapping.getManagerInfo(desiredMgrId);
        return infoOpt.map(info -> createMgrDto(desiredMgrId, entityType, info, specs.stream()
                .filter(spec -> managerMapping.getManagerUuid(spec.getName())
                    .map(name -> name.equals(desiredMgrId))
                    .orElse(false))
                .collect(Collectors.toList())));

    }

    /**
     * Convert a {@link SettingsPolicyApiDTO} received as input to create a new setting policy to
     * a matching {@link SettingPolicyInfo} that can be used inside XL.
     *
     * @param apiInputPolicy The {@link SettingsPolicyApiDTO} received from the user.
     * @return The {@link SettingPolicyInfo} for use inside XL.
     * @throws InvalidOperationException If the input is illegal.
     * @throws UnknownObjectException If the groups the setting policy is scoped to don't exist.
     */
    @Nonnull
    public SettingPolicyInfo convertNewInputPolicy(@Nonnull final SettingsPolicyApiDTO apiInputPolicy)
            throws InvalidOperationException, UnknownObjectException {
        return convertInputPolicy(apiInputPolicy, resolveEntityType(apiInputPolicy));
    }

    /**
     * Convert a {@link SettingsPolicyApiDTO} received as input to edit an existing setting policy to
     * a matching {@link SettingPolicyInfo} that can be used inside XL.
     *
     * @param existingPolicyId The ID of the existing policy.
     * @param newPolicy The {@link SettingsPolicyApiDTO} received from the user.
     * @return The {@link SettingPolicyInfo} for use inside XL.
     * @throws InvalidOperationException If the input is illegal.
     * @throws UnknownObjectException If the existing policy doesn't exist.
     */
    @Nonnull
    public SettingPolicyInfo convertEditedInputPolicy(final long existingPolicyId,
                                                      @Nonnull final SettingsPolicyApiDTO newPolicy)
            throws InvalidOperationException, UnknownObjectException {

        final GetSettingPolicyResponse getResponse =
                settingPolicyService.getSettingPolicy(GetSettingPolicyRequest.newBuilder()
                        .setId(existingPolicyId)
                        .build());
        if (!getResponse.hasSettingPolicy()) {
            throw new UnknownObjectException("Setting policy not found: " + existingPolicyId);
        }
        return convertInputPolicy(newPolicy,
                getResponse.getSettingPolicy().getInfo().getEntityType());
    }

    /**
     * Convert a {@link SettingsPolicyApiDTO} received as input to create a setting policy into
     * a matching {@link SettingPolicyInfo} that can be used inside XL.
     *
     * @param apiInputPolicy The {@link SettingsPolicyApiDTO} received from the user.
     * @param entityType The entity type of the {@link SettingsPolicyApiDTO}. This needs to be
     *                   provided externally because it's usually not set in the input DTO.
     * @return The {@link SettingPolicyInfo} representing the input DTO for internal XL
     *         communication.
     * @throws InvalidOperationException If the input is illegal.
     */
    @Nonnull
    @VisibleForTesting
    SettingPolicyInfo convertInputPolicy(@Nonnull final SettingsPolicyApiDTO apiInputPolicy,
                                         final int entityType)
            throws InvalidOperationException {
        final Map<String, SettingSpec> specsByName = new HashMap<>();
        Map<String, SettingApiDTO> involvedSettings = new HashMap<>();
        if (apiInputPolicy.getSettingsManagers() != null) {
                involvedSettings = apiInputPolicy.getSettingsManagers().stream()
                    .filter(settingMgr -> settingMgr.getSettings() != null)
                    .flatMap(settingMgr -> settingMgr.getSettings().stream())
                    .collect(Collectors.toMap(SettingApiDTO::getUuid, Function.identity()));

            // **HAAACK** for RATE_OF_RESIZE
            // If the RATE_OF_RESIZE setting has been updated as part of the Default
            // VM Setting, remove it from the SettingPolicy and update the
            // setting.
            String rateOfResizeSettingName = GlobalSettingSpecs.RateOfResize.getSettingName();
            // Api is not setting the default flag. So we can only check if it is a VM
            // type.
            if (isVmEntityType(entityType) &&
                    involvedSettings.containsKey(rateOfResizeSettingName)) {
                SettingApiDTO<?> settingApiDto = involvedSettings.remove(rateOfResizeSettingName);
                settingService.updateGlobalSetting(
                    UpdateGlobalSettingRequest.newBuilder()
                        .setSettingSpecName(rateOfResizeSettingName)
                        .setNumericSettingValue(
                            SettingDTOUtil.createNumericSettingValue(
                                Float.valueOf(SettingsMapper.inputValueToString(settingApiDto).orElse("0"))))
                        .build());
            }

            if (!involvedSettings.isEmpty()) {
                settingService.searchSettingSpecs(SearchSettingSpecsRequest.newBuilder()
                        .addAllSettingSpecName(involvedSettings.keySet())
                        .build())
                        .forEachRemaining(spec -> specsByName.put(spec.getName(), spec));
            }

            // Technically this shouldn't happen because we only return settings we support
            // from the SettingsService.
            if (!specsByName.keySet().equals(involvedSettings.keySet())) {
                throw new InvalidOperationException("Attempted to create a settings policy with " +
                        "invalid specs: " + Sets.difference(
                            involvedSettings.keySet(), specsByName.keySet()));
            }
        }

        String inputPolicyDisplayName = apiInputPolicy.getDisplayName();
        final SettingPolicyInfo.Builder infoBuilder = SettingPolicyInfo.newBuilder()
            .setName(inputPolicyDisplayName)
            .setDisplayName(inputPolicyDisplayName);

        if (!apiInputPolicy.isDefault() && apiInputPolicy.getScopes() != null) {
            final Scope.Builder scopeBuilder = Scope.newBuilder();
            apiInputPolicy.getScopes().stream()
                    .map(GroupApiDTO::getUuid)
                    .map(Long::parseLong)
                    .forEach(scopeBuilder::addGroups);
            infoBuilder.setScope(scopeBuilder);
        }

        infoBuilder.setEntityType(entityType);

        infoBuilder.setEnabled(apiInputPolicy.getDisabled() == null || !apiInputPolicy.getDisabled());

        if (apiInputPolicy.getSchedule() != null) {
            infoBuilder.setSchedule(convertScheduleApiDTO(apiInputPolicy.getSchedule()));
        }
        involvedSettings.values()
            .stream()
            .map(settingApiDto -> {
                // We expect the SettingSpec to be found.
                final SettingSpec spec = specsByName.get(settingApiDto.getUuid());
                if (spec == null) {
                    throw new IllegalArgumentException("Spec " + settingApiDto.getUuid() +
                            " not found in the specs given to the mapper.");
                }
                return toProtoSetting(settingApiDto, spec);
            }).forEach(infoBuilder::addSettings);

        return infoBuilder.build();
    }

    /**
     * Convert a Schedule DTO from the classic API to one that can be used by an XL SettingPolicyInfo.
     * A schedule may contain a recurrence DTO that in XL may indicate a one-time, daily, weekly, or
     * monthly policy. However in classic the recurrence DTO can only be of type daily, weekly, or
     * monthly, with its complete absence denoting a one-time policy.
     *
     * @param apiSchedule The classic API schedule to convert
     * @return the equivalent XL Schedule object
     */
    private Schedule convertScheduleApiDTO(ScheduleApiDTO apiSchedule) {
        final Schedule.Builder scheduleBuilder = Schedule.newBuilder();
        final long startTimestamp = DateTimeUtil.parseTime(apiSchedule.getStartTime());
        final Date startDateTime = new Date(startTimestamp);
        scheduleBuilder.setStartTime(startTimestamp)
                .setEndTime(DateTimeUtil.parseTime(apiSchedule.getEndTime()));

        if (apiSchedule.getRecurrence() == null || apiSchedule.getRecurrence().getType() == null) {
            scheduleBuilder.setOneTime(Schedule.OneTime.newBuilder().build());
        } else {
            if (apiSchedule.getEndDate() != null) {
                scheduleBuilder.setLastDate(DateTimeUtil.parseTime(apiSchedule.getEndDate()));
            } else {
                scheduleBuilder.setPerpetual(Schedule.Perpetual.newBuilder().build());
            }
            switch (apiSchedule.getRecurrence().getType()) {
                case DAILY:
                    scheduleBuilder.setDaily(Schedule.Daily.newBuilder().build());
                    break;
                case WEEKLY:
                    final Schedule.Weekly.Builder weeklyBuilder = Schedule.Weekly.newBuilder();
                    final List<DayOfWeek> apiDays = apiSchedule.getRecurrence().getDaysOfWeek();
                    if (apiDays == null || apiDays.isEmpty()) {
                        final int weekdayNumber = startDateTime.toInstant()
                                        .atZone(ZoneId.systemDefault())
                                        .toLocalDate().getDayOfWeek().getValue();
                        weeklyBuilder.addDaysOfWeek(Schedule.DayOfWeek.forNumber(weekdayNumber));
                    } else {
                        weeklyBuilder.addAllDaysOfWeek(apiDays.stream()
                                .map(this::translateDayOfWeekFromDTO)
                                .collect(Collectors.toList()));
                    }
                    scheduleBuilder.setWeekly(weeklyBuilder.build());
                    break;
                case MONTHLY:
                    final Schedule.Monthly.Builder monthlyBuilder = Schedule.Monthly.newBuilder();
                    final List<Integer> daysMonth = apiSchedule.getRecurrence().getDaysOfMonth();
                    if (daysMonth == null || daysMonth.isEmpty()) {
                        monthlyBuilder.addDaysOfMonth(startDateTime.toInstant()
                                        .atZone(ZoneId.systemDefault())
                                        .toLocalDate().getDayOfMonth());
                    } else {
                        monthlyBuilder.addAllDaysOfMonth(daysMonth);
                    }
                    scheduleBuilder.setMonthly(monthlyBuilder.build());
                    break;
            }
        }
        return scheduleBuilder.build();
    }

    /**
     * Translate the day of the week from the classic API enum to the XL recurrence enum.
     *
     * @param day the classic API day of the week
     * @return the equivalent XL day of the week
     */
    @VisibleForTesting
    Schedule.DayOfWeek translateDayOfWeekFromDTO(DayOfWeek day) {
        //legacy week starts with Sunday but international/XL week starts with Monday
        final int internationalDayNumber = (day == DayOfWeek.Sun) ? Schedule.DayOfWeek.SUNDAY_VALUE
                : day.getValue() - 1;
        return Schedule.DayOfWeek.forNumber(internationalDayNumber);
    }

    /**
     * Convert a list of {@link SettingPolicy} objects to {@link SettingsPolicyApiDTO}s that
     * can be returned to API clients.
     *
     * @param settingPolicies The setting policies retrieved from the group components.
     * @return A list of {@link SettingsPolicyApiDTO} objects in the same order as the input list.
     */
    public List<SettingsPolicyApiDTO> convertSettingPolicies(
            @Nonnull final List<SettingPolicy> settingPolicies) {
        return convertSettingPolicies(settingPolicies, Collections.emptySet());
    }

    /**
     * Convert a list of {@link SettingPolicy} objects to {@link SettingsPolicyApiDTO}s that
     * can be returned to API clients.
     *
     * @param settingPolicies The setting policies retrieved from the group components.
     * @param managersToInclude the set of managers to include in the response, if
     *                          managersToInclude is empty, return all managers
     * @return A list of {@link SettingsPolicyApiDTO} objects in the same order as the input list.
     */
    public List<SettingsPolicyApiDTO> convertSettingPolicies(
            @Nonnull final List<SettingPolicy> settingPolicies,
            @Nonnull final Set<String> managersToInclude) {
        final Set<Long> involvedGroups = SettingDTOUtil.getInvolvedGroups(settingPolicies);
        final Map<Long, String> groupNames = new HashMap<>();
        if (!involvedGroups.isEmpty()) {
            groupService.getGroups(GetGroupsRequest.newBuilder()
                    .setGroupFilter(GroupFilter.newBuilder()
                            .addAllId(involvedGroups))
                    .build())
                    .forEachRemaining(group -> groupNames.put(group.getId(),
                            group.getDefinition().getDisplayName()));
        }

        Map<String, Setting> globalSettingNameToSettingMap = getRelevantGlobalSettings(settingPolicies);

        return settingPolicies.stream()
                .map(policy -> settingPolicyMapper.convertSettingPolicy(policy, groupNames,
                        globalSettingNameToSettingMap, managersToInclude))
                .collect(Collectors.toList());
    }

    /**
     * Convert a {@link SettingPolicy} object to a {@link SettingsPolicyApiDTO} that can
     * be returned to API clients.
     *
     * @param settingPolicy The setting policy retrieved from the group component.
     * @return The list of {@link SettingsPolicyApiDTO}
     */
    public SettingsPolicyApiDTO convertSettingPolicy(
            @Nonnull final SettingPolicy settingPolicy) {
        return convertSettingPolicies(Collections.singletonList(settingPolicy)).get(0);
    }

    @VisibleForTesting
    SettingsManagerMapping getManagerMapping() {
        return managerMapping;
    }

    /**
     * Create a {@link SettingsManagerApiDTO} containing information about settings, but not their
     * values. This will be a "thicker" manager than the one created by
     * {@link DefaultSettingPolicyMapper#createValMgrDto(String, SettingsManagerInfo, String, Collection)},
     * but none of the settings will have values (since this is only a description of what the
     * settings are).
     *
     * @param mgrId The ID of the manager.
     * @param entityType An {@link Optional} containing the entity type to limit returned
     *                   {@link SettingApiDTO}s to. This is necessary because a {@link SettingSpec}
     *                   may be associated with multiple entity types.
     * @param info  The information about the manager.
     * @param specs The {@link SettingSpec}s managed by this manager. This function assumes all
     *              the settings belong to the manager.
     * @return The {@link SettingsManagerApiDTO}.
     */
    @Nonnull
    private SettingsManagerApiDTO createMgrDto(@Nonnull final String mgrId,
                                               @Nonnull final Optional<String> entityType,
                                               @Nonnull final SettingsManagerInfo info,
                                               @Nonnull final Collection<SettingSpec> specs) {
        final SettingsManagerApiDTO mgrApiDto = info.newApiDTO(mgrId);
        List<SettingSpec> sortedSpecs = info.sortSettingSpecs(specs, SettingSpec::getName);

        mgrApiDto.setSettings(sortedSpecs.stream()
                .map(settingSpec -> settingSpecMapper.settingSpecToApi(Optional.of(settingSpec), Optional.empty()))
                .flatMap(settingPossibilities -> {
                    if (entityType.isPresent()) {
                        return settingPossibilities.getSettingForEntityType(entityType.get())
                                .map(Stream::of)
                                .orElseGet(Stream::empty);
                    } else {
                        return settingPossibilities.getAll().stream();
                    }
                })
                .collect(Collectors.toList()));

        // configure UI presentation information
        for (SettingApiDTO apiDto : mgrApiDto.getSettings()) {
            String specName = apiDto.getUuid();
            settingSpecStyleMapping.getStyleInfo(specName)
                    .ifPresent(styleInfo ->
                            apiDto.setRange(styleInfo.getRange().getRangeApiDTO()));
        }

        return mgrApiDto;
    }

    /**
     * A functional interface to allow separate testing for the actual conversion, and the
     * code preceding the conversion (e.g. getting the involved groups).
     */
    @FunctionalInterface
    @VisibleForTesting
    interface SettingPolicyMapper {
        /**
         * Convert a {@link SettingPolicy} to a {@link SettingsPolicyApiDTO} that can be returned
         * to API clients.
         *
         * @param settingPolicy The {@link SettingPolicy}.
         * @param groupNames A map of group names containing all groups the policy is scoped to. This
         *                   is required to set the display names of the groups (which the API needs).
         *                   Group IDs that are not found in the map will not be included in the
         *                   resulting {@link SettingsPolicyApiDTO}.
         * @param globalSettingNameToSettingMap required global settings (like disableAllActions) that need to be
         *                                      injected to shown in UI with entity settings.
         * @param managersToInclude the set of managers to include in the response, if
         *                          managersToInclude is empty, return all managers
         * @return The resulting {@link SettingsPolicyApiDTO}.
         */
        SettingsPolicyApiDTO convertSettingPolicy(@Nonnull final SettingPolicy settingPolicy,
                                                  @Nonnull final Map<Long, String> groupNames,
                                                  @Nonnull final Map<String, Setting> globalSettingNameToSettingMap,
                                                  @Nonnull final Set<String> managersToInclude);
    }

    /**
     * The default/actual implementation of {@link SettingPolicyMapper}.
     */
    static class DefaultSettingPolicyMapper implements SettingPolicyMapper {

        private final SettingsMapper mapper;
        private final SettingServiceBlockingStub settingServiceClient;

        DefaultSettingPolicyMapper(@Nonnull final SettingsMapper mapper,
                                   @Nonnull final SettingServiceBlockingStub settingServiceClient) {

            this.mapper = mapper;
            this.settingServiceClient = settingServiceClient;
        }

        @Override
        public SettingsPolicyApiDTO convertSettingPolicy(@Nonnull final SettingPolicy settingPolicy,
                                                         @Nonnull final Map<Long, String> groupNames,
                                                         @Nonnull final Map<String, Setting> globalSettingNameToSettingMap,
                                                         @Nonnull final Set<String> managersToInclude) {
            final SettingsPolicyApiDTO apiDto = new SettingsPolicyApiDTO();
            apiDto.setUuid(Long.toString(settingPolicy.getId()));
            final SettingPolicyInfo info = settingPolicy.getInfo();
            apiDto.setDisplayName(info.hasDisplayName() ? info.getDisplayName() : info.getName());
            // We need this check because some inject some fake setting policies without any entity
            // type to show in UI (like Global Action Mode Defaults.)
            if (info.hasEntityType()) {
                apiDto.setEntityType(UIEntityType.fromType(info.getEntityType()).apiStr());
            }
            apiDto.setDisabled(!info.getEnabled());
            apiDto.setDefault(settingPolicy.getSettingPolicyType().equals(Type.DEFAULT));
            apiDto.setReadOnly(settingPolicy.getSettingPolicyType().equals(Type.DISCOVERED));

            if (info.hasScope()) {
                apiDto.setScopes(info.getScope().getGroupsList().stream()
                        .map(groupId -> {
                            String groupName = groupNames.get(groupId);
                            if (groupName != null) {
                                GroupApiDTO group = new GroupApiDTO();
                                group.setUuid(Long.toString(groupId));
                                group.setDisplayName(groupName);
                                return Optional.of(group);
                            } else {
                                logger.warn("Group {} in scope of policy {} not found!", groupId,
                                        info.getName());
                                return Optional.<GroupApiDTO>empty();
                            }
                        })
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList()));
            }

            if (info.hasSchedule()) {
                apiDto.setSchedule(convertScheduleToApiDTO(info.getSchedule()));
            }

            final SettingsManagerMapping managerMapping = mapper.getManagerMapping();

            // Do the actual settings mapping.
            final Map<String, List<Setting>> settingsByMgr = info.getSettingsList()
                    .stream()
                    .collect(Collectors.groupingBy(setting ->
                            managerMapping.getManagerUuid(setting.getSettingSpecName())
                                    .orElse(NO_MANAGER)));

            // *HAAACK*. Add RATE_OF_RESIZE hack here. Even thought it is a global setting
            // in the UI, it is displayed along with VM default policy. So fetch the global
            // setting and inject it here.
            if (isVmDefaultPolicy(settingPolicy)) {
                injectGlobalSetting(GlobalSettingSpecs.RateOfResize, settingsByMgr,
                                globalSettingNameToSettingMap);
            } else if (isGlobalActionModePolicy(settingPolicy)) {
                apiDto.setEntityType(SERVICE_ENTITY);
                injectGlobalSetting(GlobalSettingSpecs.DisableAllActions, settingsByMgr,
                                globalSettingNameToSettingMap);
            }

            final List<Setting> unhandledSettings = settingsByMgr.get(NO_MANAGER);
            if (unhandledSettings != null && !unhandledSettings.isEmpty()) {
                logger.warn("The following settings don't have a mapping in the API component." +
                        " Not returning them to the user. Settings: {}", unhandledSettings.stream()
                        .map(Setting::getSettingSpecName)
                        .collect(Collectors.toSet()));
            }

            apiDto.setSettingsManagers(settingsByMgr.entrySet().stream()
                    // Don't return the specs that don't have a Manager mapping
                    .filter(entry -> !entry.getKey().equals(NO_MANAGER))
                    // only return settings for requested managers, if managers is empty, return all
                    .filter(entry -> managersToInclude.isEmpty() || managersToInclude.contains(entry.getKey()))
                    .map(entry -> {
                        final SettingsManagerInfo mgrInfo = managerMapping.getManagerInfo(entry.getKey())
                                .orElseThrow(() -> new IllegalStateException("Manager ID " +
                                        entry.getKey() + " not found despite being in the mappings earlier."));
                        return createValMgrDto(entry.getKey(), mgrInfo, apiDto.getEntityType(), entry.getValue());
                    })
                    .collect(Collectors.toList()));

            return apiDto;
        }

        /**
         * Fetches the global setting for given setting spec and injects it in settings by Manager map.
         * @param settingSpecs to be injected.
         * @param settingsByMgr map of manager and associated settings.
         */
        private void injectGlobalSetting(GlobalSettingSpecs settingSpecs,
                        Map<String, List<Setting>> settingsByMgr,
                        final Map<String, Setting> globalSettingNameToSettingMap) {
            Setting globalSetting = globalSettingNameToSettingMap.get(settingSpecs.getSettingName());
            if (globalSetting != null) {
                settingsByMgr.computeIfAbsent(
                                mapper.getManagerMapping()
                                    .getManagerUuid(settingSpecs.getSettingName()).orElse(NO_MANAGER),
                                k -> new ArrayList<>())
                            .add(globalSetting);
           } else {
                logger.error("No global setting {} found! This should not happen.",
                                settingSpecs.getSettingName());
           }
        }

        /**
         * Convert an XL Schedule object to the Schedule DTO used by the classic API.
         *
         * @param schedule the XL Schedule object
         * @return an equivalent ScheduleApiDTO
         */
        private ScheduleApiDTO convertScheduleToApiDTO(Schedule schedule) {
            final ScheduleApiDTO scheduleApiDTO = new ScheduleApiDTO();

            final long startTimestamp = schedule.getStartTime();
            final Date startDateTime = new Date(startTimestamp);
            final String startTimeString = DateTimeUtil.toString(startTimestamp);

            scheduleApiDTO.setStartDate(startTimeString);
            scheduleApiDTO.setStartTime(startTimeString);

            switch (schedule.getDurationCase()) {
                case MINUTES:
                    final Date endDateTime = Date.from(startDateTime.toInstant().plus(schedule.getMinutes(), ChronoUnit.MINUTES));
                    scheduleApiDTO.setEndTime(DateTimeUtil.toString(endDateTime));
                    break;
                case END_TIME:
                    scheduleApiDTO.setEndTime(DateTimeUtil.toString(schedule.getEndTime()));
                    break;
            }

            if (schedule.hasLastDate()) {
                scheduleApiDTO.setEndDate(DateTimeUtil.toString(schedule.getLastDate()));
            }

            final RecurrenceApiDTO recurrenceApiDTO = new RecurrenceApiDTO();
            switch (schedule.getRecurrenceCase()) {
                case DAILY:
                    recurrenceApiDTO.setType(RecurrenceType.DAILY);
                    scheduleApiDTO.setRecurrence(recurrenceApiDTO);
                    break;
                case WEEKLY:
                    recurrenceApiDTO.setType(RecurrenceType.WEEKLY);
                    if (schedule.getWeekly().getDaysOfWeekList().isEmpty()) {
                        recurrenceApiDTO.setDaysOfWeek(Collections.singletonList(
                                getLegacyDayOfWeekForDatestamp(startDateTime)));
                    } else {
                        recurrenceApiDTO.setDaysOfWeek(schedule.getWeekly().getDaysOfWeekList()
                                .stream().map(this::translateDayOfWeekToDTO)
                                .collect(Collectors.toList()));
                    }
                    scheduleApiDTO.setRecurrence(recurrenceApiDTO);
                    break;
                case MONTHLY:
                    recurrenceApiDTO.setType(RecurrenceType.MONTHLY);
                    if (schedule.getMonthly().getDaysOfMonthList().isEmpty()) {
                        final int dayOfTheMonth = startDateTime.toInstant()
                                        .atZone(ZoneId.systemDefault()).toLocalDate().getDayOfMonth();
                        recurrenceApiDTO.setDaysOfMonth(Collections.singletonList(
                                        dayOfTheMonth));
                    } else {
                        recurrenceApiDTO.setDaysOfMonth(schedule.getMonthly().getDaysOfMonthList());
                    }
                    scheduleApiDTO.setRecurrence(recurrenceApiDTO);
                    break;
            }
            return scheduleApiDTO;
        }

        /**
         * Translate an XL DayOfWeek enum to the DayOfWeek enum used by the classic API.
         *
         * @param day the XL DayOfWeek
         * @return the corresponding classic DayOfWeek
         */
        private DayOfWeek translateDayOfWeekToDTO(Schedule.DayOfWeek day) {
            return DayOfWeek.get(day.name().substring(0, 3));
        }

        /**
         * Get the api.enums.DayOfWeek associated with a date represented by a Joda DateTime.
         *
         * @param dateTime the datetime to convert
         * @return the legacy DayOfWeek associated
         */
        @VisibleForTesting
        DayOfWeek getLegacyDayOfWeekForDatestamp(@Nonnull final Date dateTime) {
            final int dayOfWeekInternational = dateTime.toInstant()
                        .atOffset(ZoneOffset.UTC)
                        .toLocalDate().getDayOfWeek().getValue();
            //legacy enum week starts with sunday but DateTime week starts with monday
            final int dayOfWeekLegacy =
                    dayOfWeekInternational == java.time.DayOfWeek.SUNDAY.getValue() ? 1
                    : dayOfWeekInternational + 1;
            return DayOfWeek.get(dayOfWeekLegacy);
        }

        /**
         * Create a {@link SettingsManagerApiDTO} containing setting values to return to the API.
         * This will be a "thinner" manager - all the settings will only have the UUIDs and values.
         *
         * @param mgrId The ID of the setting manager.
         * @param mgrInfo The {@link SettingsManagerInfo} for the manager.
         * @param settings The {@link Setting} objects containing setting values. All of these settings
         *                 should be "managed" by this manager, but this method does not check.
         * @return The {@link SettingsManagerApiDTO}.
         */
        @Nonnull
        @VisibleForTesting
        SettingsManagerApiDTO createValMgrDto(@Nonnull final String mgrId,
                                              @Nonnull final SettingsManagerInfo mgrInfo,
                                              @Nonnull final String entityType,
                                              @Nonnull final Collection<Setting> settings) {
            final SettingsManagerApiDTO mgrApiDto = mgrInfo.newApiDTO(mgrId);

            mgrApiDto.setSettings(settings.stream()
                    .map(setting -> mapper.toSettingApiDto(setting).getSettingForEntityType(entityType))
                    .filter(Optional::isPresent).map(Optional::get)
                    .collect(Collectors.toList()));
            return mgrApiDto;
        }
    }

    /**
     * Create a {@link SettingApiDTO} for the API from a {@link Setting} object.
     *
     * @param setting The {@link Setting} object representing the value of a particular setting.
     * @return The {@link SettingApiDTO} representing the value of the setting.
     */
    @Nonnull
    public SettingApiDTOPossibilities toSettingApiDto(@Nonnull final Setting setting) {
        // UI needs both the setting and its setting spec to be set.
        final Optional<SettingSpec> settingSpec = getSettingSpec(setting.getSettingSpecName());
        if (!settingSpec.isPresent()) {
            logger.warn("Could not find setting spec for setting {}", setting.getSettingSpecName());
        }
        return settingSpecMapper.settingSpecToApi(settingSpec, Optional.of(setting));
    }

    @Nonnull
    public SettingApiDTOPossibilities toSettingApiDto(@Nonnull final Setting setting,
                                                      @Nonnull final SettingSpec settingSpec) {
        return settingSpecMapper.settingSpecToApi(Optional.of(settingSpec), Optional.of(setting));
    }

    /**
     * Create a {@link Setting} object from a {@link SettingApiDTO}.
     *
     * @param apiDto The {@link SettingApiDTO} received from the API user.
     * @param settingSpec  The spec for the setting. We need the spec to know how to interret the
     *                     value.
     * @return The {@link Setting}.
     */
    @Nonnull
    private static Setting toProtoSetting(@Nonnull final SettingApiDTO<?> apiDto,
                                          @Nonnull final SettingSpec settingSpec) {
        final Setting.Builder settingBuilder = Setting.newBuilder()
                .setSettingSpecName(apiDto.getUuid());
        PROTO_SETTING_VALUE_INJECTORS.get(settingSpec.getSettingValueTypeCase())
                .setBuilderValue(StringUtils.trimToEmpty(SettingsMapper.inputValueToString(apiDto).orElse("")), settingBuilder);
        return settingBuilder.build();
    }

    /**
     * Map a set of {@link SettingApiDTO}s to their associated {@link Setting} objects.
     * The input DTO's should have values.
     *
     * @param apiDtos The collection of {@link SettingApiDTO}s to map.
     * @return The {@link Setting} objects, arranged by setting name (UUID in API terms). Any
     *         settings that could not be mapped will not be present.
     */
    @Nonnull
    public Map<SettingApiDtoKey, Setting> toProtoSettings(@Nonnull final List<SettingApiDTO<String>> apiDtos) {
        final Map<String, List<SettingApiDTO>> settingOverrides = apiDtos.stream()
                .collect(Collectors.groupingBy(SettingApiDTO::getUuid));

        final ImmutableMap.Builder<SettingApiDtoKey, Setting> retChanges = ImmutableMap.builder();
        // We need to look up the setting specs to know how to apply setting overrides.
        settingService.searchSettingSpecs(SearchSettingSpecsRequest.newBuilder()
                .addAllSettingSpecName(settingOverrides.keySet())
                .build())
                .forEachRemaining(settingSpec -> {
                    // Since settings in XL are uniquely identified by name, the name of XL's "Setting"
                    // object is the same as the UUID of the API's "SettingApiDTO" object.
                    final List<SettingApiDTO> dtos = settingOverrides.get(settingSpec.getName());
                    dtos.stream().forEach(dto -> {
                        try {
                            retChanges.put(getSettingApiDtoKey(dto), toProtoSetting(dto, settingSpec));
                        } catch (RuntimeException e) {
                            // Catch and log any runtime exceptions to isolate the failure to that
                            // particular misbehaving setting.
                            logger.error("Unable to map setting " + settingSpec.getName() +
                                    " because of error.", e);
                        }
                    });

                });
        return retChanges.build();
    }

    /**
     * Used to generate immurtable key for {@link SettingApiDTO}.
     */
    @Value.Immutable
    public interface SettingApiDtoKey {
        //SettingApiDtoUuid
        String settingUUID();

        //SettingApiDto entityType mapped to UIEntityType
        UIEntityType entityType();
    }

    /**
     * Gets {@link SettingApiDtoKey} from {@link SettingApiDTO}.
     *
     * @param dto to used to build SettingApiDtoKey
     * @return key created from settingApiDto
     */
    public static SettingApiDtoKey getSettingApiDtoKey(SettingApiDTO dto) {
        return ImmutableSettingApiDtoKey.builder()
                .entityType(UIEntityType.fromString(dto.getEntityType()))
                .settingUUID(dto.getUuid())
                .build();
    }


    /**
     * Injects a String value into a {@link Setting.Builder}. The injector is responsible for
     * selecting the right type of value, and reinterpreting the string input as that type.
     */
    @FunctionalInterface
    private interface ProtoSettingValueInjector {
        void setBuilderValue(@Nonnull final String value, @Nonnull final Setting.Builder builder);
    }

    /**
     * Injects a value from a {@link Setting} into an {@link SettingApiDTO}, formatted as a string,
     * and sets the value type of the {@link SettingApiDTO} according to the value type of the
     * setting.
     */
    @FunctionalInterface
    private interface ApiSettingValueInjector {
        void setSettingValue(@Nonnull final Setting setting, @Nonnull final SettingApiDTO<String> apiDTO);
    }

    /**
     * The mapper from {@link SettingSpec} to {@link SettingApiDTO}s, extracted as an interface
     * for unit testing/mocking purposes.
     */
    @FunctionalInterface
    @VisibleForTesting
    interface SettingSpecMapper {
        /**
         * Convert a {@link SettingSpec} to all possible {@link SettingApiDTO}s that can be derived
         * from that setting spec.
         *
         * @param settingSpec Optional {@link SettingSpec}. If not present, there will be no
         *                    {@link SettingApiDTO}s in the response.
         * @param setting Optional {@link Setting} containing an actual value for the setting. If
         *                present, all {@link SettingApiDTO}s in the response will contain the value.
         *                If not present, all {@link SettingApiDTO}s in the response will have no
         *                value set (i.e. {@link SettingApiDTO#getValue()} will return null).
         * @return The {@link SettingApiDTOPossibilities} describing the possible
         *         {@link SettingApiDTO}s that can be derived from the input setting spec.
         */
        @Nonnull
        SettingApiDTOPossibilities settingSpecToApi(@Nonnull final Optional<SettingSpec> settingSpec,
                                                    @Nonnull final Optional<Setting> setting);
    }

    /**
     * The "real" implementation of {@link SettingSpecMapper}.
     */
    @VisibleForTesting
    static class DefaultSettingSpecMapper implements SettingSpecMapper {

        @Override
        @Nonnull
        public SettingApiDTOPossibilities settingSpecToApi(@Nonnull final Optional<SettingSpec> settingSpec,
                                                           @Nonnull final Optional<Setting> setting) {
            return new SettingApiDTOPossibilities(settingSpec, setting);
        }
    }

    public static Optional<String> inputValueToString(@Nullable SettingApiDTO<?> dto) {
        if (dto == null) {
            return Optional.empty();
        }
        Object value = dto.getValue();
        if (value == null) {
            return Optional.empty();
        }
        if (value instanceof String) {
            return Optional.of((String)value);
        }
        return Optional.of(value.toString());
    }

    private static boolean isVmDefaultPolicy(@Nonnull SettingPolicy policy) {

        return (policy.getSettingPolicyType().equals(Type.DEFAULT) &&
                    isVmEntityType(policy.getInfo().getEntityType()));
    }

    public static boolean isVmEntityType(int entityType) {
        return (EntityType.VIRTUAL_MACHINE.getNumber() == entityType);
    }

    private static boolean isGlobalActionModePolicy(SettingPolicy settingPolicy) {
        return settingPolicy.getInfo().getName().equals(GLOBAL_ACTION_SETTING_NAME);
    }

    private static Optional<SettingSpec> getSettingSpec(@Nonnull final String settingSpecName) {
        Optional<EntitySettingSpecs> entitySpec =
            EntitySettingSpecs.getSettingByName(settingSpecName);
        if (entitySpec.isPresent()) {
            return Optional.of(entitySpec.get().getSettingSpec());
        }

        Optional<GlobalSettingSpecs> globalSpec =
            GlobalSettingSpecs.getSettingByName(settingSpecName);

        if (globalSpec.isPresent()) {
            return Optional.of(globalSpec.get().createSettingSpec());
        }

        return Optional.empty();
    }

    /**
     * An object to represent the possible {@link SettingApiDTO}s associated with a single
     * {@link SettingSpec}. The {@link SettingApiDTO}s may or may not have values, depending
     * on whether a {@link Setting} is provided.
     * <p>In XL, a single {@link SettingSpec} may apply to multiple entity types. However, a
     * {@link SettingApiDTO} applies to just one entity type. There are circumstances where we need
     * all the {@link SettingApiDTO}s that can be derived from a {@link SettingSpec}, and other
     * circumstances where we only need the {@link SettingApiDTO} for a specific entity type.
     * This object is meant to provide an easy way to pass all these possibilities around when
     * mapping between XL and the API DTOs.
     */
    public static class SettingApiDTOPossibilities {

        /**
         * {@link SettingApiDTO}s arranged by entity type. This should be null if the provided
         * {@link SettingSpec} was a global setting.
         */
        @Nullable
        private final Map<String, SettingApiDTO<String>> settingsByEntityType;

        /**
         * The {@link SettingApiDTO} associated with the global {@link SettingSpec}. This should
         * be null if the provided {@link SettingSpec} was an entity setting.
         */
        @Nullable
        private final SettingApiDTO<String> globalSetting;

        private SettingApiDTOPossibilities(@Nonnull final Optional<SettingSpec> settingSpec,
                                   @Nonnull final Optional<Setting> settingValue) {
            this.settingsByEntityType = settingSpec
                    .map(spec -> makeEntitySettings(spec, settingValue)).orElse(null);
            this.globalSetting = settingSpec
                    .map(spec -> makeGlobalSetting(spec, settingValue)).orElse(null);

            Preconditions.checkArgument(!(settingsByEntityType != null && globalSetting != null),
                "A single setting spec cannot be mapped to both per-entity  and global " +
                "settings. Spec: " + settingSpec.map(SettingSpec::toString).orElse("NONE"));
        }

        /**
         * Get all possible {@link SettingApiDTO}s associated with the input {@link SettingSpec}.
         *
         * @return A collection of {@link SettingApiDTO}s. May be empty.
         */
        @Nonnull
        public Collection<SettingApiDTO<String>> getAll() {
            if (globalSetting != null) {
                return Collections.singletonList(globalSetting);
            } else if (settingsByEntityType != null) {
                return settingsByEntityType.values();
            } else {
                return Collections.emptyList();
            }
        }

        /**
         * Get the {@link SettingApiDTO} associated with the global setting represented by
         * the input {@link SettingSpec}.
         *
         * @return An {@link Optional} containing the {@link SettingApiDTO}. An empty optional if
         *         the input {@link SettingSpec} did not represent a global setting.
         */
        @Nonnull
        public Optional<SettingApiDTO<String>> getGlobalSetting() {
            return Optional.ofNullable(globalSetting);
        }

        /**
         * Get the {@link SettingApiDTO} associated with the entity setting for a particular entity
         * type represented by the input {@link SettingSpec}.
         *
         * @param entityType The UI entity type (i.e. one of {@link UIEntityType}) to search for.
         * @return An {@link Optional} containing the {@link SettingApiDTO}. An empty optional if
         *         the input {@link SettingSpec} did not represent an entity setting, or if the
         *         input {@link SettingSpec} does not apply to the specified entity type.
         */
        @Nonnull
        public Optional<SettingApiDTO<String>> getSettingForEntityType(@Nonnull final String entityType) {
            if (settingsByEntityType != null) {
                return Optional.ofNullable(settingsByEntityType.get(entityType));
            } else if (globalSetting != null) {
                return StringUtils.equals(globalSetting.getEntityType(), entityType) ?
                        Optional.of(globalSetting) : Optional.empty();
            } else {
                return Optional.empty();
            }
        }

        @Nullable
        private static SettingApiDTO<String> makeGlobalSetting(
                @Nonnull final SettingSpec settingSpec,
                @Nonnull final Optional<Setting> settingValue) {
            if (!settingSpec.hasGlobalSettingSpec()) {
                return null;
            }

            final SettingApiDTO<String> apiDto = new SettingApiDTO<>();
            apiDto.setScope(SettingScope.GLOBAL);
            apiDto.setEntityType(GLOBAL_SETTING_ENTITY_TYPES.get(settingSpec.getName()));
            fillSkeleton(settingSpec, settingValue, apiDto);
            return apiDto;
        }

        @Nullable
        private static Map<String, SettingApiDTO<String>> makeEntitySettings(
                @Nonnull final SettingSpec settingSpec,
                @Nonnull final Optional<Setting> settingValue) {
            if (!settingSpec.hasEntitySettingSpec()) {
                return null;
            }

            final Set<String> applicableTypes = new HashSet<>();
            final EntitySettingScope scope =
                    settingSpec.getEntitySettingSpec().getEntitySettingScope();
            switch (scope.getScopeCase()) {
                case ENTITY_TYPE_SET:
                    scope.getEntityTypeSet().getEntityTypeList().forEach(entityType ->
                            applicableTypes.add(UIEntityType.fromType(entityType).apiStr()));
                    break;
                case ALL_ENTITY_TYPE:
                    for (UIEntityType validType : UIEntityType.values()) {
                        applicableTypes.add(validType.apiStr());
                    }
                    break;
                default:
                    logger.error("Invalid entity scope {} for setting spec {}.",
                            scope.getScopeCase(), settingSpec);
                    return null;
            }

            return applicableTypes.stream()
                    .map(entityType -> {
                        final SettingApiDTO<String> apiDto = new SettingApiDTO<>();
                        if (settingSpec.getEntitySettingSpec().getAllowGlobalDefault()) {
                            // We explicitly want the scope unset, because for API purposes LOCAL scope
                            // settings are those that are ONLY applicable to groups. However, entity
                            // setting specs also have globally editable defaults (which are considered
                            // global), so we can't say they have LOCAL or GLOBAL scope.
                            //
                            // In the API, something that has both LOCAL and GLOBAL scope has a "null"
                            // scope at the time of this writing (Oct 10 2017) :)
                            apiDto.setScope(null);
                        } else {
                            // If a global default is NOT allowed, then it's a pure local scope.
                            apiDto.setScope(SettingScope.LOCAL);
                        }

                        apiDto.setEntityType(entityType);
                        fillSkeleton(settingSpec, settingValue, apiDto);
                        return apiDto;
                    }).collect(Collectors.toMap(SettingApiDTO::getEntityType, Function.identity()));
        }

        /**
         * A utility method to fill a {@link SettingApiDTO} with the various information
         * contained in a {@link SettingSpec} and {@link Setting}. This handles all fields common
         * to both global and entity settings.
         *
         * @param settingSpec The {@link SettingSpec} the {@link SettingApiDTO} is derived from.
         * @param settingVal An {@link Optional} containing a {@link Setting} value. May be absent
         *                   if we just want to provide a description of the setting.
         * @param dtoSkeleton A {@link SettingApiDTO} that will be modified by this method.
         */
        private static void fillSkeleton(@Nonnull final SettingSpec settingSpec,
                                         @Nonnull final Optional<Setting> settingVal,
                                         @Nonnull final SettingApiDTO<String> dtoSkeleton) {
            dtoSkeleton.setUuid(settingSpec.getName());
            dtoSkeleton.setDisplayName(settingSpec.getDisplayName());

            if (settingSpec.hasPath()) {
                final List<String> categories = new LinkedList<>();
                SettingCategoryPathNode pathNode = settingSpec.getPath().getRootPathNode();
                categories.add(pathNode.getNodeName());
                while (pathNode.hasChildNode()) {
                    pathNode = pathNode.getChildNode();
                    categories.add(pathNode.getNodeName());
                }
                dtoSkeleton.setCategories(categories);
            }

            switch (settingSpec.getSettingValueTypeCase()) {
                case BOOLEAN_SETTING_VALUE_TYPE:
                    dtoSkeleton.setValueType(InputValueType.BOOLEAN);
                    dtoSkeleton.setDefaultValue(
                            Boolean.toString(settingSpec.getBooleanSettingValueType().getDefault()));
                    break;
                case NUMERIC_SETTING_VALUE_TYPE:
                    dtoSkeleton.setValueType(InputValueType.NUMERIC);
                    final NumericSettingValueType numericType = settingSpec.getNumericSettingValueType();
                    dtoSkeleton.setDefaultValue(Float.toString(numericType.getDefault()));
                    if (numericType.hasMin()) {
                        dtoSkeleton.setMin((double)numericType.getMin());
                    }
                    if (numericType.hasMax()) {
                        dtoSkeleton.setMax((double)numericType.getMax());
                    }
                    break;
                case STRING_SETTING_VALUE_TYPE:
                    dtoSkeleton.setValueType(InputValueType.STRING);
                    final StringSettingValueType stringType = settingSpec.getStringSettingValueType();
                    dtoSkeleton.setDefaultValue(stringType.getDefault());
                    break;
                case ENUM_SETTING_VALUE_TYPE:
                    // Enum is basically a string with predefined allowable values.
                    dtoSkeleton.setValueType(InputValueType.STRING);
                    final EnumSettingValueType enumType = settingSpec.getEnumSettingValueType();
                    dtoSkeleton.setDefaultValue(enumType.getDefault());
                    dtoSkeleton.setOptions(enumType.getEnumValuesList().stream()
                            .map(enumValue -> {
                                final SettingOptionApiDTO enumOption = new SettingOptionApiDTO();
                                enumOption.setLabel(SETTING_ENUM_NAME_TO_LABEL.getOrDefault(enumValue,
                                        ActionDTOUtil.upperUnderScoreToMixedSpaces(enumValue)));
                                enumOption.setValue(enumValue);
                                return enumOption;
                            })
                            .collect(Collectors.toList()));
                    break;
                case SORTED_SET_OF_OID_SETTING_VALUE_TYPE:
                    dtoSkeleton.setValueType(InputValueType.LIST);
                    break;
                default:
                    throw new IllegalArgumentException("Illegal settingValueType " +
                        settingSpec.getSettingValueTypeCase());
            }

            settingVal.ifPresent(setting -> API_SETTING_VALUE_INJECTORS.get(setting.getValueCase())
                    .setSettingValue(setting, dtoSkeleton));
        }
    }

    /**
     * Update global action mode settings based on input setting policy received from UI.
     * Sets to default value if setDefault is true. Currently only relevant for one global action
     * mode setting GlobalSettingSpecs.DisableAllActions.
     * @param setDefault if true then set default value.
     * @param settingPolicy read value for setting and set this value.
     * @throws OperationFailedException if we are not able to set value for the setting.
     */
    public void updateGlobalActionModeSetting(boolean setDefault, SettingsPolicyApiDTO settingPolicy)
                    throws OperationFailedException {
        SettingSpec disableAllActionsSetting = GlobalSettingSpecs.DisableAllActions.createSettingSpec();
        final boolean valueToSet;
        if (setDefault) {
            valueToSet = disableAllActionsSetting.getBooleanSettingValueType().getDefault();
        } else {
            valueToSet = settingPolicy.getSettingsManagers().stream()
                    .flatMap(manager -> manager.getSettings().stream())
                    .filter(setting -> setting.getUuid().equals(disableAllActionsSetting.getName()))
                    .map(SettingsMapper::inputValueToString)
                    .filter(Optional::isPresent).map(Optional::get)
                    .map(Boolean::valueOf)
                    .findFirst()
                    .orElseThrow(() -> {
                        String errorMsg = disableAllActionsSetting.getName() +
                                " : not found in Global Settings.";
                        return new OperationFailedException(errorMsg);
                    });
        }

        settingService.updateGlobalSetting(
            UpdateGlobalSettingRequest.newBuilder()
                .setSettingSpecName(disableAllActionsSetting.getName())
                .setBooleanSettingValue(SettingDTOUtil.createBooleanSettingValue(valueToSet))
                .build());
    }

    private Map<String, Setting> getRelevantGlobalSettings(List<SettingPolicy> settingPolicies) {
        Set<String> globalSettingNames = settingPolicies.stream()
                        .map(policy -> {
                            if (isVmDefaultPolicy(policy)) {
                                return GlobalSettingSpecs.RateOfResize.getSettingName();
                            } else if (isGlobalActionModePolicy(policy)) {
                                return GlobalSettingSpecs.DisableAllActions.getSettingName();
                            } else {
                                return null;
                            }
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());

        Map<String, Setting> globalSettings = new HashMap<String, Setting>();

        if (CollectionUtils.isEmpty(globalSettingNames)) {
            return globalSettings;
        }

        try {
            final GetMultipleGlobalSettingsRequest settingRequest =
                            GetMultipleGlobalSettingsRequest.newBuilder()
                                .addAllSettingSpecName(globalSettingNames)
                                .build();
            settingService.getMultipleGlobalSettings(settingRequest)
                .forEachRemaining(setting -> {
                    globalSettings.put(setting.getSettingSpecName(), setting);
                });

            if (globalSettings.size() != globalSettingNames.size()) {
                logger.error("Failed to get requested global settings from group component."
                                + " Requested {} but received {} .",
                                globalSettingNames.size(), globalSettings.size());
            }
        } catch (StatusRuntimeException e) {
            logger.error("Failed to get global settings from group component.");
        }
        return globalSettings;
    }

    /**
     * Converts a template to a SettingsManagerApiDTO. In the SettingsManagerApiDTO we save
     * the name of the template.
     *
     * @param template the template to be converted
     * @return the SettingsManagerApiDTO with the name of the template
     */
    @Nonnull
    public SettingsManagerApiDTO toSettingsManagerApiDTO(Template template) {
        String templateName = template.getTemplateInfo().getName();

        SettingsManagerApiDTO settingsManagerApiDto = new SettingsManagerApiDTO();
        settingsManagerApiDto.setUuid("capacityplandatamanager");
        settingsManagerApiDto.setDisplayName(templateName);

        SettingApiDTO<Serializable> settingApiDTO = new SettingApiDTO();
        settingApiDTO.setDisplayName("Template Name");
        settingApiDTO.setValue(templateName);
        settingApiDTO.setValueDisplayName(templateName);
        List<SettingApiDTO<Serializable>> list = new ArrayList<>();
        list.add(settingApiDTO);
        settingsManagerApiDto.setSettings(list);

        return settingsManagerApiDto;
    }
}

