package com.vmturbo.group.setting;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

import org.apache.commons.lang3.StringUtils;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting.ValueCase;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.SettingValueTypeCase;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.group.IGroupStore;

/**
 * The default implementation of {@link SettingPolicyValidator}. This should be the
 * only implementation!
 */
@ThreadSafe
public class DefaultSettingPolicyValidator implements SettingPolicyValidator {

    private final Map<SettingValueTypeCase, BiFunction<Setting, SettingSpec, Collection<String>>>
            settingProcessors;
    private final SettingSpecStore settingSpecStore;
    private final IGroupStore groupStore;

    public DefaultSettingPolicyValidator(@Nonnull final SettingSpecStore settingSpecStore,
            @Nonnull final IGroupStore groupStore) {
        this.settingSpecStore = Objects.requireNonNull(settingSpecStore);
        this.groupStore = Objects.requireNonNull(groupStore);

        final Map<SettingValueTypeCase, BiFunction<Setting, SettingSpec, Collection<String>>>
                processors = new EnumMap<>(SettingValueTypeCase.class);
        processors.put(SettingValueTypeCase.BOOLEAN_SETTING_VALUE_TYPE,
                this::processBooleanSetting);
        processors.put(SettingValueTypeCase.ENUM_SETTING_VALUE_TYPE, this::processEnumSetting);
        processors.put(SettingValueTypeCase.STRING_SETTING_VALUE_TYPE, this::processStringSetting);
        processors.put(SettingValueTypeCase.NUMERIC_SETTING_VALUE_TYPE, this::processNumericSetting);
        processors.put(SettingValueTypeCase.SORTED_SET_OF_OID_SETTING_VALUE_TYPE, this::processSortedSetOfOidSetting);
        settingProcessors = Collections.unmodifiableMap(processors);
    }

    /**
     * {@inheritDoc}
     */
    public void validateSettingPolicy(@Nonnull final SettingPolicyInfo settingPolicyInfo,
            @Nonnull final Type type) throws InvalidItemException {
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
            if (!setting.hasSettingSpecName()) {
                errors.add("Setting for spec " + specName + " has unset name field.");
            } else if (StringUtils.isBlank(specName)) {
                errors.add("Null/empty key in setting spec map!");
            }
        });

        errors.addAll(validateReferencedSpecs(settingPolicyInfo));

        if (type.equals(Type.DEFAULT)) {
            if (settingPolicyInfo.hasScope()) {
                errors.add("Default setting policy should not have a scope!");
            }
            if (settingPolicyInfo.hasScheduleId()) {
                errors.add("Default setting policy should not have a schedule.");
            }
            if (settingPolicyInfo.hasTargetId()) {
                errors.add("Default setting policy must not have a targetId.");
            }
        } else if (type.equals(Type.USER)) {
            if (settingPolicyInfo.hasTargetId()) {
                errors.add("User setting policy must not have a targetId.");
            }

            if (!settingPolicyInfo.hasScope() ||
                    settingPolicyInfo.getScope().getGroupsCount() < 1) {
                // as of OM-44888, we are no longer making scopes required, and will not longer
                // generate an error here.
            } else {
                // Make sure the groups exist, and are compatible with the policy info.
                try {
                    final Set<Long> policyGroups =
                            new HashSet<>(settingPolicyInfo.getScope().getGroupsList());
                    final Collection<Grouping> groups = groupStore.getGroups(
                            GroupProtoUtil.createGroupFilterByIds(policyGroups));
                    groups.stream().map(Grouping::getId).forEach(policyGroups::remove);
                    if (!policyGroups.isEmpty()) {
                        errors.add("Groups " + policyGroups + " for setting policy not found");
                    }
                    for (Grouping group: groups) {
                        final int policyEntityType = settingPolicyInfo.getEntityType();
                        final Collection<Integer> groupExpectedMemberTypes =
                                GroupProtoUtil.getEntityTypes(group)
                                        .stream()
                                        .map(UIEntityType::typeNumber)
                                        .collect(Collectors.toSet());
                        if (!groupExpectedMemberTypes.contains(policyEntityType)) {
                            errors.add("Group " + group.getId() + " with entity type " +
                                    groupExpectedMemberTypes + " does not match entity type " +
                                    policyEntityType + " of the setting policy");
                        }
                    }
                } catch (DataAccessException e) {
                    errors.add("Unable to fetch groups for setting policy due to exception: " +
                            e.getMessage());
                }
            }
        } else {
            if (!settingPolicyInfo.hasTargetId()) {
                Preconditions.checkArgument(type == Type.DISCOVERED);
                errors.add("Discovered setting policy must set the target_id field.");
            }
        }

        if (!errors.isEmpty()) {
            throw new InvalidItemException(
                    "Invalid setting policy: " + settingPolicyInfo.getName() +
                            System.lineSeparator() +
                            StringUtils.join(errors, System.lineSeparator()));
        }
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

    /**
     * Validate settings with {@link ValueCase.SORTED_SET_OF_OID_SETTING_VALUE}.
     * The list value of the setting should be sorted in natural order and
     * shouldn't contain any duplicate, which means it's strictly ordered.
     *
     * @param setting the {@link Setting} to validate
     * @param settingSpec the {@link SettingSpec} corresponds to the setting
     * @return a {@link Collection} of errors
     */
    private Collection<String> processSortedSetOfOidSetting(@Nonnull Setting setting,
                                                            @Nonnull SettingSpec settingSpec) {
        final Optional<String> typeError = matchType(setting, ValueCase.SORTED_SET_OF_OID_SETTING_VALUE);
        if (typeError.isPresent()) {
            return Collections.singleton(typeError.get());
        }
        final List<Long> value = setting.getSortedSetOfOidSettingValue().getOidsList();
        if (!Ordering.natural().isStrictlyOrdered(value)) {
            return Collections.singleton("Value " + value + " is not strictly ordered.");
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
