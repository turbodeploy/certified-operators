package com.vmturbo.group.persistent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.DurationCase;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.EndingCase;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.RecurrenceCase;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting.ValueCase;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.SettingValueTypeCase;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;

/**
 * The default implementation of {@link SettingPolicyValidator}. This should be the
 * only implementation!
 */
@ThreadSafe
public class DefaultSettingPolicyValidator implements SettingPolicyValidator {

    private final Map<SettingValueTypeCase, BiFunction<Setting, SettingSpec, Collection<String>>>
            settingProcessors;
    private final SettingSpecStore settingSpecStore;
    private final GroupStore groupStore;

    public DefaultSettingPolicyValidator(@Nonnull final SettingSpecStore settingSpecStore,
            @Nonnull final GroupStore groupStore) {
        this.settingSpecStore = Objects.requireNonNull(settingSpecStore);
        this.groupStore = Objects.requireNonNull(groupStore);

        final Map<SettingValueTypeCase, BiFunction<Setting, SettingSpec, Collection<String>>>
                processors = new EnumMap<>(SettingValueTypeCase.class);
        processors.put(SettingValueTypeCase.BOOLEAN_SETTING_VALUE_TYPE,
                this::processBooleanSetting);
        processors.put(SettingValueTypeCase.ENUM_SETTING_VALUE_TYPE, this::processEnumSetting);
        processors.put(SettingValueTypeCase.STRING_SETTING_VALUE_TYPE, this::processStringSetting);
        processors.put(SettingValueTypeCase.NUMERIC_SETTING_VALUE_TYPE,
                this::processNumericSetting);
        settingProcessors = Collections.unmodifiableMap(processors);
    }

    /**
     * {@inheritDoc}
     */
    public void validateSettingPolicy(@Nonnull final SettingPolicyInfo settingPolicyInfo,
            @Nonnull final Type type) throws InvalidSettingPolicyException {
        // We want to collect everything wrong with the input and put that
        // into the description message.
        final List<String> errors = new LinkedList<>();

        if (!settingPolicyInfo.hasName()) {
            errors.add("Setting policy must have a name!");
        }

        if (!settingPolicyInfo.hasEntityType()) {
            errors.add("Setting policy must have an entity type!");
        }

        settingPolicyInfo.getSettingsList().forEach((setting) -> {
            final String specName = setting.getSettingSpecName();
            if (StringUtils.isBlank(specName)) {
                errors.add("Null/empty key in setting spec map!");
            } else if (!setting.hasSettingSpecName()) {
                errors.add("Setting for spec " + specName + " has unset name field.");
            } else if (!specName.equals(setting.getSettingSpecName())) {
                errors.add("Spec name " + specName +
                        " mapped to setting with inconsistent name: " +
                        setting.getSettingSpecName());
            }
        });

        errors.addAll(validateReferencedSpecs(settingPolicyInfo));

        if (type.equals(Type.DEFAULT)) {
            if (settingPolicyInfo.hasScope()) {
                errors.add("Default setting policy should not have a scope!");
            }
            if (settingPolicyInfo.hasSchedule()) {
                errors.add("Default setting policy should not have a schedule.");
            }
        } else {
            if (settingPolicyInfo.hasSchedule()) {
                errors.addAll(validateSchedule(settingPolicyInfo.getSchedule()));
            }

            if (!settingPolicyInfo.hasScope() ||
                    settingPolicyInfo.getScope().getGroupsCount() < 1) {
                errors.add("User setting policy must have at least one scope!");
            } else {
                // Make sure the groups exist, and are compatible with the policy info.
                try {
                    final Map<Long, Optional<Group>> groupMap =
                            groupStore.getGroups(settingPolicyInfo.getScope().getGroupsList());
                    groupMap.forEach((groupId, groupOpt) -> {
                        if (groupOpt.isPresent()) {
                            final Group group = groupOpt.get();
                            final int policyEntityType = settingPolicyInfo.getEntityType();
                            final int groupEntityType = GroupProtoUtil.getEntityType(group);
                            if (groupEntityType != policyEntityType) {
                                errors.add("Group " + group.getId() + " with entity type " +
                                        groupEntityType + " does not match entity type " +
                                        policyEntityType + " of the setting policy");
                            }
                        } else {
                            errors.add("Group " + groupId + "for setting policy not found.");
                        }
                    });
                } catch (DatabaseException e) {
                    errors.add("Unable to fetch groups for setting policy due to exception: " +
                            e.getMessage());
                }
            }
        }

        if (!errors.isEmpty()) {
            throw new InvalidSettingPolicyException(
                    "Invalid setting policy: " + settingPolicyInfo.getName() +
                            System.lineSeparator() +
                            StringUtils.join(errors, System.lineSeparator()));
        }
    }

    private List<String> validateSchedule(@Nonnull final Schedule schedule) {
        List<String> errors = new LinkedList<>();

        if (!schedule.hasStartTime()) {
            errors.add("Setting policy schedule must have start datetime.");
        }
        if (schedule.getDurationCase().equals(DurationCase.DURATION_NOT_SET)) {
            errors.add("Setting policy schedule must have a duration consisting of " +
                    "either an end time or a number of minutes.");
        }
        if (schedule.hasEndTime() && (schedule.getStartTime() >= schedule.getEndTime())) {
            errors.add("Setting policy schedule end time must be after start time.");
        }
        if (schedule.hasMinutes() && (schedule.getMinutes() < 1)) {
            errors.add("Setting policy schedule duration must be one minute or greater.");
        }
        if (schedule.getRecurrenceCase().equals(RecurrenceCase.RECURRENCE_NOT_SET)) {
            errors.add("Setting policy schedule recurrence must be one of OneTime, " +
                    "Daily, Weekly, or Monthly.");
        }
        if (schedule.hasOneTime() &&
                !schedule.getEndingCase().equals(EndingCase.ENDING_NOT_SET)) {
            errors.add("OneTime setting policy schedule cannot have end date or " +
                    "be perpetual.");
        }
        if (!schedule.hasOneTime() &&
                schedule.getEndingCase().equals(EndingCase.ENDING_NOT_SET)) {
            errors.add("Recurring setting policy schedule must have end date or " +
                    "be perpetual.");
        }
        if (schedule.hasLastDate() && schedule.getStartTime() > schedule.getLastDate()) {
            errors.add("Last date of recurring setting policy must be after first date.");
        }
        if (schedule.hasWeekly() && schedule.getWeekly().getDaysOfWeekList().isEmpty()) {
            errors.add("Weekly setting policy schedule must have at least one active day.");
        }
        if (schedule.hasMonthly()){
            if (schedule.getMonthly().getDaysOfMonthList().isEmpty()) {
                errors.add("Monthly setting policy schedule must have at least one active day.");
            }
            schedule.getMonthly().getDaysOfMonthList().forEach(day -> {
                if ((day < 1)  || (day > 31)) {
                    errors.add("Monthly setting policy schedule can only have " +
                            "active day(s) 1-31. " + day + " is invalid.");
                }
            });
        }

        return errors;
    }

    @Nonnull
    private List<String> validateReferencedSpecs(
            @Nonnull final SettingPolicyInfo settingPolicyInfo) {
        // We want to collect everything wrong with the input.
        final List<String> errors = new LinkedList<>();

        final Map<Setting, Optional<SettingSpec>> referencedSpecs =
                settingPolicyInfo.getSettingsList()
                        .stream()
                        .filter(Setting::hasSettingSpecName)
                        .collect(Collectors.toMap(Function.identity(),
                                setting -> settingSpecStore.getSettingSpec(
                                        setting.getSettingSpecName())));
        referencedSpecs.forEach((setting, specOpt) -> {
            if (!specOpt.isPresent()) {
                errors.add("Setting " + setting.getSettingSpecName() + " does not exist!");
            } else {
                final String name = setting.getSettingSpecName();
                final SettingSpec spec = specOpt.get();
                if (!spec.hasEntitySettingSpec()) {
                    errors.add("Setting " + name + " is not an entity setting, " +
                            "and can't be overwritten by a setting policy!");
                } else {
                    // Make sure that the input policy info matches any
                    // entity type restrictions in the setting scope.
                    final int entityType = settingPolicyInfo.getEntityType();
                    final EntitySettingScope scope =
                            spec.getEntitySettingSpec().getEntitySettingScope();
                    if (scope.hasEntityTypeSet() && !scope.getEntityTypeSet()
                            .getEntityTypeList()
                            .contains(entityType)) {
                        errors.add("Entity type " + entityType +
                                " not supported by setting spec " + name +
                                ". Must be one of: " +
                                StringUtils.join(scope.getEntityTypeSet().getEntityTypeList(),
                                        ", "));
                    }
                }

                final BiFunction<Setting, SettingSpec, Collection<String>> processor =
                        settingProcessors.get(spec.getSettingValueTypeCase());
                errors.addAll(processor.apply(setting, spec));
            }
        });
        return errors;
    }

    private Collection<String> processBooleanSetting(@Nonnull Setting setting,
            @Nonnull SettingSpec settingSpec) {
        final Optional<String> typeError = matchType(setting, ValueCase.BOOLEAN_SETTING_VALUE);
        if (typeError.isPresent()) {
            return Collections.singleton(typeError.get());
        } else {
            return Collections.emptySet();
        }
    }

    private Collection<String> processNumericSetting(@Nonnull Setting setting,
            @Nonnull SettingSpec settingSpec) {
        final Optional<String> typeError = matchType(setting, ValueCase.NUMERIC_SETTING_VALUE);
        if (typeError.isPresent()) {
            return Collections.singleton(typeError.get());
        }
        final NumericSettingValueType type = settingSpec.getNumericSettingValueType();
        final float value = setting.getNumericSettingValue().getValue();
        final Collection<String> errors = new ArrayList<>(0);
        if (type.hasMin() && value < type.getMin()) {
            errors.add("Value " + value + " for setting " + setting.getSettingSpecName() +
                    " less than minimum!");
        }
        if (type.hasMax() && value > type.getMax()) {
            errors.add("Value " + value + " for setting " + setting.getSettingSpecName() +
                    " more than maximum!");
        }
        return errors;
    }

    private Collection<String> processStringSetting(@Nonnull Setting setting,
            @Nonnull SettingSpec settingSpec) {
        final Optional<String> typeError = matchType(setting, ValueCase.STRING_SETTING_VALUE);
        if (typeError.isPresent()) {
            return Collections.singleton(typeError.get());
        }
        final StringSettingValueType type = settingSpec.getStringSettingValueType();
        final String value = setting.getStringSettingValue().getValue();
        if (type.hasValidationRegex() &&
                !Pattern.compile(type.getValidationRegex()).matcher(value).matches()) {
            return Collections.singleton("Value " + value + " does not match validation regex " +
                    type.getValidationRegex());
        }
        return Collections.emptySet();
    }

    private Collection<String> processEnumSetting(@Nonnull Setting setting,
            @Nonnull SettingSpec settingSpec) {
        final Optional<String> typeError = matchType(setting, ValueCase.ENUM_SETTING_VALUE);
        if (typeError.isPresent()) {
            return Collections.singleton(typeError.get());
        }
        final EnumSettingValueType type = settingSpec.getEnumSettingValueType();
        final String value = setting.getEnumSettingValue().getValue();
        if (!type.getEnumValuesList().contains(value)) {
            return Collections.singleton("Value " + value + " is not in the allowable list: " +
                    StringUtils.join(type.getEnumValuesList(), ", "));
        }
        return Collections.emptySet();
    }

    private Optional<String> matchType(@Nonnull Setting setting, ValueCase valueCase) {
        if (setting.getValueCase() != valueCase) {
            return Optional.of(
                    "Mismatched value. Got " + setting.getValueCase() + " and expected " +
                            valueCase);
        } else {
            return Optional.empty();
        }
    }
}
