package com.vmturbo.common.protobuf;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;

/**
 * Utilities for dealing with messages defined in {@link SettingProto} (Setting.proto).
 */
public final class SettingDTOUtil {


    private SettingDTOUtil() {
    }

    /**
     * Get a set of groups involved in a collection of {@link SettingPolicy} objects.
     *
     * @param settingPolicies The input collection of {@link SettingPolicy} objects.
     * @return A set containing the union of all groups the {@link SettingPolicy} objects apply to.
     *         Default setting policies have no effect on the returned list.
     */
    public static Set<Long> getInvolvedGroups(@Nonnull final Collection<SettingPolicy> settingPolicies) {
        return settingPolicies.stream()
                .map(SettingPolicy::getInfo)
                .filter(SettingPolicyInfo::hasScope)
                .map(SettingPolicyInfo::getScope)
                .flatMap(scope -> scope.getGroupsList().stream())
                .collect(Collectors.toSet());
    }

    /**
     * Get the groups involved in a single {@link SettingPolicy}.
     *
     * @param settingPolicy See {@link SettingDTOUtil#getInvolvedGroups(Collection)}.
     * @return See {@link SettingDTOUtil#getInvolvedGroups(Collection)}.
     */
    public static Set<Long> getInvolvedGroups(@Nonnull final SettingPolicy settingPolicy) {
        return getInvolvedGroups(Collections.singleton(settingPolicy));
    }

    /**
     * Get the set of entity types that are common to all the input {@link SettingSpec}s.
     *
     * @param settingSpecs The {@link SettingSpec}s to examine.
     * @return An optional of the set of entity types common to the input {@link SettingSpec}s.
     *         An empty optional if no input {@link SettingSpec}s explicitly specify entity types.
     *         This could happen if, for example, all of them are a mix of global and "applicable to
     *         all entity types".
     */
    public static Optional<Set<Integer>> getOverlappingEntityTypes(
            @Nonnull final Collection<SettingSpec> settingSpecs) {

        final List<Set<Integer>> explicitEntityTypes = settingSpecs.stream()
            .filter(SettingSpec::hasEntitySettingSpec)
            .map(spec -> spec.getEntitySettingSpec().getEntitySettingScope())
            .filter(EntitySettingScope::hasEntityTypeSet)
            .map(EntitySettingScope::getEntityTypeSet)
            .map(EntityTypeSet::getEntityTypeList)
            .map(Sets::newHashSet)
            .collect(Collectors.toList());

        if (explicitEntityTypes.isEmpty()) {
            // There are no specs that explicitly specify entity types.
            return Optional.empty();
        } else {
            final Set<Integer> intersectedSet = Sets.newHashSet(explicitEntityTypes.get(0));
            for (int i = 1; i < explicitEntityTypes.size(); ++i) {
                intersectedSet.retainAll(explicitEntityTypes.get(i));
            }
            return Optional.of(intersectedSet);
        }
    }

    /**
     *  Return the default setting policies from the input list.
     *
     *  @param settingPolicies List of SettingPolicy.
     *  @return List of Default SettingPolicy.
     */
    public static List<SettingPolicy> extractDefaultSettingPolicies(List<SettingPolicy> settingPolicies) {
        return settingPolicies.stream()
            .filter(settingPolicy -> settingPolicy.hasSettingPolicyType() &&
                settingPolicy.getSettingPolicyType() == SettingPolicy.Type.DEFAULT)
            .collect(Collectors.toList());
    }

    /**
     *  Return the user setting policies from the input list.
     *
     *  @param settingPolicies List of SettingPolicy.
     *  @return List of User SettingPolicy.
     */
    public static List<SettingPolicy> extractUserSettingPolicies(List<SettingPolicy> settingPolicies) {
        return settingPolicies.stream()
            .filter(settingPolicy -> settingPolicy.hasSettingPolicyType() &&
                settingPolicy.getSettingPolicyType() == SettingPolicy.Type.USER)
            .collect(Collectors.toList());
    }

    /**
     * Create a mapping from EntityType to SettingPolicy.
     *
     * @param settingPolicies List of SettingPolicy messages.
     * @return Map of EntityType to SettingPolicyId.
     *
     */
    public static Map<Integer, SettingPolicy> arrangeByEntityType(List<SettingPolicy> settingPolicies) {
        return settingPolicies.stream()
            .filter(sp -> sp.hasInfo() && sp.getInfo().hasEntityType())
            .collect(Collectors.toMap(sp -> sp.getInfo().getEntityType(), Function.identity()));

    }

    /**
     * Compare two EnumSettingValue types.
     *
     * @param value1 EnumSettingValue.
     * @param value2 EnumSettingValue.
     * @param type EnumSettingValueType.
     * @return Positive, negative or zero integer where value1 is
     *         greater than, smaller than or equal to value2 respectively.
     *
     */
    public static int compareEnumSettingValues(EnumSettingValue value1,
                                               EnumSettingValue value2,
                                               EnumSettingValueType type) {
        return (type.getEnumValuesList().indexOf(value1.getValue())
                - type.getEnumValuesList().indexOf(value2.getValue()));
    }
}
