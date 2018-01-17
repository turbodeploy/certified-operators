package com.vmturbo.api.component.external.api.mapper;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.grpc.Channel;

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
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
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
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Responsible for mapping Settings-related XL objects to their API counterparts.
 */
public class SettingsMapper {

    private static final Logger logger = LogManager.getLogger();

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
                .setValue(val)))
            .build();

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
            .build();

    /**
     * A constant indicating that a {@link SettingSpec} has no associated setting manager.
     */
    private static final String NO_MANAGER = "";

    private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

    private final SettingsManagerMapping managerMapping;

    private final SettingSpecStyleMapping settingSpecStyleMapping;

    private final SettingSpecMapper settingSpecMapper;

    private final SettingPolicyMapper settingPolicyMapper;

    private final GroupServiceBlockingStub groupService;

    private final SettingServiceBlockingStub settingService;

    private final SettingPolicyServiceBlockingStub settingPolicyService;

    public SettingsMapper(@Nonnull final Channel groupComponentChannel,
                          @Nonnull final SettingsManagerMapping settingsManagerMapping,
                          @Nonnull final SettingSpecStyleMapping settingSpecStyleMapping) {
        this.managerMapping = settingsManagerMapping;
        this.settingSpecStyleMapping = settingSpecStyleMapping;
        this.settingSpecMapper = new DefaultSettingSpecMapper();
        this.groupService = GroupServiceGrpc.newBlockingStub(groupComponentChannel);
        this.settingService = SettingServiceGrpc.newBlockingStub(groupComponentChannel);
        this.settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(groupComponentChannel);
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
        final Iterable<Group> groups = () -> groupService.getGroups(GetGroupsRequest.newBuilder()
                .addAllId(groupIds)
                .build());
        final Map<Integer, List<Group>> groupsByEntityType =
            StreamSupport.stream(groups.spliterator(), false)
                .collect(Collectors.groupingBy(GroupProtoUtil::getEntityType));
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
     * @return The {@link SettingsManagerApiDTO}s.
     */
    public List<SettingsManagerApiDTO> toManagerDtos(@Nonnull final Collection<SettingSpec> specs) {
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
                    return createMgrDto(entry.getKey(), info, entry.getValue());
                })
                .collect(Collectors.toList());
    }

    /**
     * Convert a collection of {@link SettingSpec} objects into a specific
     * {@link SettingsManagerApiDTO}. This is like {@link SettingsMapper#toManagerDtos(Collection)},
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
            @Nonnull final String desiredMgrId) {
        final Optional<SettingsManagerInfo> infoOpt = managerMapping.getManagerInfo(desiredMgrId);
        return infoOpt.map(info -> createMgrDto(desiredMgrId, info, specs.stream()
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
                SettingApiDTO settingApiDto = involvedSettings.remove(rateOfResizeSettingName);
                settingService.updateGlobalSetting(
                    UpdateGlobalSettingRequest.newBuilder()
                        .setSettingSpecName(rateOfResizeSettingName)
                        .setNumericSettingValue(
                            SettingDTOUtil.createNumericSettingValue(
                                Float.valueOf(settingApiDto.getValue())))
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

        final SettingPolicyInfo.Builder infoBuilder = SettingPolicyInfo.newBuilder()
            .setName(apiInputPolicy.getDisplayName());

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
            }).forEach(setting -> infoBuilder.addSettings(
                        setting));

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

        scheduleBuilder.setStartTime(DateTimeUtil.parseTime(apiSchedule.getStartTime()))
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
                        final LocalDateTime dateTime = makeDateTime(scheduleBuilder.getStartTime());
                        final String weekday = dateTime.getDayOfWeek().name();
                        weeklyBuilder.addDaysOfWeek(Schedule.DayOfWeek.valueOf(weekday));
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
                        final LocalDateTime dateTime = makeDateTime(scheduleBuilder.getStartTime());
                        monthlyBuilder.addDaysOfMonth(dateTime.getDayOfMonth());
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
     * Translate the day of the week from the classic API enum to the XL recurrence enum
     *
     * @param day the classic API day of the week
     * @return the equivalent XL day of the week
     */
    private Schedule.DayOfWeek translateDayOfWeekFromDTO(DayOfWeek day) {
        List<Schedule.DayOfWeek> relevantDay = Arrays.stream(Schedule.DayOfWeek.values())
                .filter(dayEnum ->
                        dayEnum.name().toLowerCase().startsWith(day.getName().toLowerCase()))
                .collect(Collectors.toList());
        return relevantDay.get(0);
    }

    /**
     * Translate a Unix timestamp to a java.time.LocalDateTime that info can be extracted from
     *
     * @param millisSinceEpoch the Unix timestamp in milliseconds in UTC
     * @return the equivalent java.time.LocalDateTime
     */
    static  LocalDateTime makeDateTime(final long millisSinceEpoch) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(millisSinceEpoch), UTC_ZONE_ID);
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
        final Set<Long> involvedGroups = SettingDTOUtil.getInvolvedGroups(settingPolicies);
        final Map<Long, String> groupNames = new HashMap<>();
        if (!involvedGroups.isEmpty()) {
            groupService.getGroups(GetGroupsRequest.newBuilder()
                    .addAllId(involvedGroups)
                    .build())
                    .forEachRemaining(group -> groupNames.put(group.getId(),
                            GroupProtoUtil.getGroupName(group)));
        }

        return settingPolicies.stream()
            .map(policy -> settingPolicyMapper.convertSettingPolicy(policy, groupNames))
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
     * {@link DefaultSettingPolicyMapper#createValMgrDto(String, SettingsManagerInfo, Collection)},
     * but none of the settings will have values (since this is only a description of what the
     * settings are).
     *
     * @param mgrId The ID of the manager.
     * @param info The information about the manager.
     * @param specs The {@link SettingSpec}s managed by this manager. This function assumes all
     *              the settings belong to the manager.
     * @return The {@link SettingsManagerApiDTO}.
     */
    @Nonnull
    private SettingsManagerApiDTO createMgrDto(@Nonnull final String mgrId,
                                        @Nonnull final SettingsManagerInfo info,
                                        @Nonnull final Collection<SettingSpec> specs) {
        final SettingsManagerApiDTO mgrApiDto = info.newApiDTO(mgrId);

        mgrApiDto.setSettings(specs.stream()
                .map(spec -> settingSpecMapper.settingSpecToApi(spec))
                .filter(Optional::isPresent).map(Optional::get)
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
         * @return The resulting {@link SettingsPolicyApiDTO}.
         */
        SettingsPolicyApiDTO convertSettingPolicy(@Nonnull final SettingPolicy settingPolicy,
                                                  @Nonnull final Map<Long, String> groupNames);
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
                                                         @Nonnull final Map<Long, String> groupNames) {
            final SettingsPolicyApiDTO apiDto = new SettingsPolicyApiDTO();
            apiDto.setUuid(Long.toString(settingPolicy.getId()));

            final SettingPolicyInfo info = settingPolicy.getInfo();
            apiDto.setDisplayName(info.getName());
            apiDto.setEntityType(ServiceEntityMapper.toUIEntityType(info.getEntityType()));
            apiDto.setDisabled(!info.getEnabled());
            apiDto.setDefault(settingPolicy.getSettingPolicyType().equals(Type.DEFAULT));

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
                String rateOfResizeSettingName = GlobalSettingSpecs.RateOfResize.getSettingName();
                Setting rateOfResizeSetting = settingServiceClient.getGlobalSetting(
                    GetSingleGlobalSettingRequest.newBuilder()
                        .setSettingSpecName(rateOfResizeSettingName).build());
                settingsByMgr.computeIfAbsent(
                            managerMapping.getManagerUuid(rateOfResizeSettingName).orElse(NO_MANAGER),
                                k -> new ArrayList<Setting>())
                               .add(rateOfResizeSetting);
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
                    .map(entry -> {
                        final SettingsManagerInfo mgrInfo = managerMapping.getManagerInfo(entry.getKey())
                                .orElseThrow(() -> new IllegalStateException("Manager ID " +
                                        entry.getKey() + " not found despite being in the mappings earlier."));
                        return createValMgrDto(entry.getKey(), mgrInfo, entry.getValue());
                    })
                    .collect(Collectors.toList()));

            return apiDto;
        }

        /**
         * Convert an XL Schedule object to the Schedule DTO used by the classic API.
         *
         * @param schedule the XL Schedule object
         * @return an equivalent ScheduleApiDTO
         */
        private ScheduleApiDTO convertScheduleToApiDTO(Schedule schedule) {
            final ScheduleApiDTO scheduleApiDTO = new ScheduleApiDTO();
            final long startTime = schedule.getStartTime();

            // Schedule datetimes are only visible in the UI if DateTimeFormatter.ISO_DATE_TIME is used.
            // If the datetimes are in Unix millisecond format, they are stored but not visible.
            // ಠ_ಠ
            scheduleApiDTO.setStartDate(makeDateTime(startTime)
                    .format(DateTimeFormatter.ISO_DATE_TIME));
            scheduleApiDTO.setStartTime(makeDateTime(startTime)
                    .format(DateTimeFormatter.ISO_DATE_TIME));

            switch (schedule.getDurationCase()) {
                case MINUTES:
                    final LocalDateTime endDateTime = makeDateTime(startTime)
                            .plusMinutes(schedule.getMinutes());
                    scheduleApiDTO.setEndTime(endDateTime.format(DateTimeFormatter.ISO_DATE_TIME));
                    break;
                case END_TIME:
                    scheduleApiDTO.setEndTime(makeDateTime(schedule.getEndTime())
                            .format(DateTimeFormatter.ISO_DATE_TIME));
                    break;
            }

            if (schedule.hasLastDate()) {
                scheduleApiDTO.setEndDate(makeDateTime(schedule.getLastDate())
                        .format(DateTimeFormatter.ISO_DATE_TIME));
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
                                getDayOfWeekForDatestamp(schedule.getStartTime())));
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
                        recurrenceApiDTO.setDaysOfMonth(
                                Collections.singletonList(makeDateTime(startTime).getDayOfMonth()));
                    } else {
                        recurrenceApiDTO.setDaysOfMonth(schedule.getMonthly().getDaysOfMonthList());
                    }
                    scheduleApiDTO.setRecurrence(recurrenceApiDTO);
                    break;
            }
            return scheduleApiDTO;

        }

        /**
         * Translate an XL DayOfWeek enum to the DayOfWeek enum used by the classic API
         * @param day the XL DayOfWeek
         * @return the corresponding classic DayOfWeek
         */
        private DayOfWeek translateDayOfWeekToDTO(Schedule.DayOfWeek day) {
            return DayOfWeek.valueOf(StringUtils.capitalize(
                    day.name().substring(0,3).toLowerCase()));
        }

        /**
         * Get the name of the day of the week associated with a date represented by a
         * Unix millisecond datetime.
         * @param datestamp the Unix millisecond datetime
         * @return the day name of the associated date
         */
        private DayOfWeek getDayOfWeekForDatestamp(long datestamp) {
            return DayOfWeek.valueOf(StringUtils.capitalize(
                    makeDateTime(datestamp).getDayOfWeek().name().substring(0,3).toLowerCase()));
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
                                              @Nonnull final Collection<Setting> settings) {
            final SettingsManagerApiDTO mgrApiDto = mgrInfo.newApiDTO(mgrId);

            mgrApiDto.setSettings(settings.stream()
                    .map(SettingsMapper::toSettingApiDto)
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
    public static SettingApiDTO toSettingApiDto(@Nonnull final Setting setting) {
        SettingApiDTO apiDto = new SettingApiDTO();
        apiDto.setUuid(setting.getSettingSpecName());
        Optional<SettingSpec> spec = getSettingSpec(setting.getSettingSpecName());
        Optional<SettingApiDTO> dto;

        // UI needs both the setting and its setting spec to be set.
        if (spec.isPresent()) {
            dto = (new DefaultSettingSpecMapper()).settingSpecToApi(spec.get());
            if (dto.isPresent()) {
                apiDto = dto.get();
            }
        }

        API_SETTING_VALUE_INJECTORS.get(setting.getValueCase()).setSettingValue(setting, apiDto);
        return apiDto;
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
    private static Setting toProtoSetting(@Nonnull final SettingApiDTO apiDto,
                                          @Nonnull final SettingSpec settingSpec) {
        final Setting.Builder settingBuilder = Setting.newBuilder()
                .setSettingSpecName(apiDto.getUuid());
        PROTO_SETTING_VALUE_INJECTORS.get(settingSpec.getSettingValueTypeCase())
                .setBuilderValue(apiDto.getValue(), settingBuilder);
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
    public Map<String, Setting> toProtoSettings(@Nonnull final List<SettingApiDTO> apiDtos) {
        final Map<String, SettingApiDTO> settingOverrides = apiDtos.stream()
                .collect(Collectors.toMap(SettingApiDTO::getUuid, Function.identity()));

        final ImmutableMap.Builder<String, Setting> retChanges = ImmutableMap.builder();
        // We need to look up the setting specs to know how to apply setting overrides.
        settingService.searchSettingSpecs(SearchSettingSpecsRequest.newBuilder()
                .addAllSettingSpecName(settingOverrides.keySet())
                .build())
                .forEachRemaining(settingSpec -> {
                    // Since settings in XL are uniquely identified by name, the name of XL's "Setting"
                    // object is the same as the UUID of the API's "SettingApiDTO" object.
                    final SettingApiDTO dto = settingOverrides.get(settingSpec.getName());
                    try {
                        retChanges.put(settingSpec.getName(), toProtoSetting(dto, settingSpec));
                    } catch (RuntimeException e) {
                        // Catch and log any runtime exceptions to isolate the failure to that
                        // particular misbehaving setting.
                        logger.error("Unable to map setting " + settingSpec.getName() +
                                " because of error.", e);
                    }
                });
        return retChanges.build();
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
        void setSettingValue(@Nonnull final Setting setting, @Nonnull final SettingApiDTO apiDTO);
    }


    /**
     * The mapper from {@link SettingSpec} to {@link SettingApiDTO}, extracted as an interface
     * for unit testing/mocking purposes.
     */
    @FunctionalInterface
    @VisibleForTesting
    interface SettingSpecMapper {
        Optional<SettingApiDTO> settingSpecToApi(@Nonnull final SettingSpec settingSpec);
    }

    /**
     * The "real" implementation of {@link SettingSpecMapper}.
     */
    @VisibleForTesting
    static class DefaultSettingSpecMapper implements SettingSpecMapper {

        @Override
        public Optional<SettingApiDTO> settingSpecToApi(@Nonnull final SettingSpec settingSpec) {
            final SettingApiDTO apiDTO = new SettingApiDTO();
            apiDTO.setUuid(settingSpec.getName());
            apiDTO.setDisplayName(settingSpec.getDisplayName());

            if (settingSpec.hasPath()) {
                final List<String> categories = new LinkedList<>();
                SettingCategoryPathNode pathNode = settingSpec.getPath().getRootPathNode();
                categories.add(pathNode.getNodeName());
                while (pathNode.hasChildNode()) {
                    pathNode = pathNode.getChildNode();
                    categories.add(pathNode.getNodeName());
                }
                apiDTO.setCategories(categories);
            }

            switch (settingSpec.getSettingTypeCase()) {
                case ENTITY_SETTING_SPEC:
                    final EntitySettingScope entityScope =
                            settingSpec.getEntitySettingSpec().getEntitySettingScope();
                    // We explicitly want the scope unset, because for API purposes LOCAL scope
                    // settings are those that are ONLY applicable to groups. However, entity
                    // setting specs also have globally editable defaults (which are considered
                    // global), so we can't say they have LOCAL or GLOBAL scope.
                    //
                    // In the API, something that has both LOCAL and GLOBAL scope has a "null"
                    // scope at the time of this writing (Oct 10 2017) :)
                    if (settingSpec.getEntitySettingSpec().getAllowGlobalDefault()) {
                        apiDTO.setScope(null);
                    } else {
                        apiDTO.setScope(SettingScope.LOCAL);
                    }

                    // (Nov 2017) In XL, Setting Specs can support multiple entity types.
                    // The SettingApiDTO only supports one type. Therefore, we set the entity type
                    // to null, and rely on the caller (SettingsService) to set the entity type
                    // based on the entity type the API/UI requested settings for.
                    apiDTO.setEntityType(null);
                    break;
                case GLOBAL_SETTING_SPEC:
                    apiDTO.setScope(SettingScope.GLOBAL);
                    break;
            }

            switch (settingSpec.getSettingValueTypeCase()) {
                case BOOLEAN_SETTING_VALUE_TYPE:
                    apiDTO.setValueType(InputValueType.BOOLEAN);
                    apiDTO.setDefaultValue(
                            Boolean.toString(settingSpec.getBooleanSettingValueType().getDefault()));
                    break;
                case NUMERIC_SETTING_VALUE_TYPE:
                    apiDTO.setValueType(InputValueType.NUMERIC);
                    final NumericSettingValueType numericType = settingSpec.getNumericSettingValueType();
                    apiDTO.setDefaultValue(Float.toString(numericType.getDefault()));
                    if (numericType.hasMin()) {
                        apiDTO.setMin((double) numericType.getMin());
                    }
                    if (numericType.hasMax()) {
                        apiDTO.setMax((double) numericType.getMax());
                    }
                    break;
                case STRING_SETTING_VALUE_TYPE:
                    apiDTO.setValueType(InputValueType.STRING);
                    final StringSettingValueType stringType = settingSpec.getStringSettingValueType();
                    apiDTO.setDefaultValue(stringType.getDefault());
                    break;
                case ENUM_SETTING_VALUE_TYPE:
                    // Enum is basically a string with predefined allowable values.
                    apiDTO.setValueType(InputValueType.STRING);
                    final EnumSettingValueType enumType = settingSpec.getEnumSettingValueType();
                    apiDTO.setDefaultValue(enumType.getDefault());
                    apiDTO.setOptions(enumType.getEnumValuesList().stream()
                            .map(enumValue -> {
                                final SettingOptionApiDTO enumOption = new SettingOptionApiDTO();
                                enumOption.setLabel(enumValue);
                                enumOption.setValue(enumValue);
                                return enumOption;
                            })
                            .collect(Collectors.toList()));
                    break;
            }

            return Optional.of(apiDTO);
        }
    }

    private static boolean isVmDefaultPolicy(@Nonnull SettingPolicy policy) {

        return (policy.getSettingPolicyType().equals(Type.DEFAULT) &&
                    isVmEntityType(policy.getInfo().getEntityType()));
    }

    private static boolean isVmEntityType(int entityType) {
        return (EntityType.VIRTUAL_MACHINE.getNumber() == entityType);
    }

    private static Optional<SettingSpec> getSettingSpec(@Nonnull String settingSpecName) {
        Optional<EntitySettingSpecs> entitySpec =
            EntitySettingSpecs.getSettingByName(settingSpecName);
        if (entitySpec.isPresent()) {
            return Optional.of(entitySpec.get().createSettingSpec());
        }

        Optional<GlobalSettingSpecs> globalSpec =
            GlobalSettingSpecs.getSettingByName(settingSpecName);

        if (globalSpec.isPresent()) {
            return Optional.of(globalSpec.get().createSettingSpec());
        }

        return Optional.empty();
    }
}

