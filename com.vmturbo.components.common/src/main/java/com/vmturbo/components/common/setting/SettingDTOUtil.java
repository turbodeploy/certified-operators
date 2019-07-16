package com.vmturbo.components.common.setting;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingsForEntity;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;

/**
 * Utilities for dealing with messages defined in {@link SettingProto} (Setting.proto).
 */
public final class SettingDTOUtil {


    private SettingDTOUtil() {
    }

    /**
     * Method constructs setting category path protobuf from the categoryPath variable.
     *
     * @return {@link SettingCategoryPath} object.
     */
    @Nonnull
    public static SettingCategoryPath createSettingCategoryPath(@Nonnull List<String> categoryPath) {
        Objects.requireNonNull(categoryPath);
        final ListIterator<String> categoryIterator =
                categoryPath.listIterator(categoryPath.size());
        SettingCategoryPath.SettingCategoryPathNode childNode = null;
        while (categoryIterator.hasPrevious()) {
            final SettingCategoryPath.SettingCategoryPathNode.Builder nodeBuilder =
                    SettingCategoryPath.SettingCategoryPathNode.newBuilder()
                            .setNodeName(categoryIterator.previous());
            if (childNode != null) {
                nodeBuilder.setChildNode(childNode);
            }
            childNode = nodeBuilder.build();
        }
        final SettingCategoryPath.Builder builder = SettingCategoryPath.newBuilder();
        if (childNode != null) {
            builder.setRootPathNode(childNode);
        }
        return builder.build();
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
    public static List<SettingPolicy> extractDefaultSettingPolicies(Collection<SettingPolicy> settingPolicies) {
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
    public static List<SettingPolicy> extractUserAndDiscoveredSettingPolicies(Collection<SettingPolicy> settingPolicies) {
        return settingPolicies.stream()
            .filter(settingPolicy -> settingPolicy.hasSettingPolicyType() &&
                (settingPolicy.getSettingPolicyType() == SettingPolicy.Type.USER ||
                    settingPolicy.getSettingPolicyType() == SettingPolicy.Type.DISCOVERED))
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
     * Convert an iterator over {@link GetEntitySettingsResponse} objects (returned by
     * a gRPC call) to a stream of the contained {@link SettingsForEntity} objects.
     *
     * @param iterator The iterator returned by the gRPC call - represents the server stream.
     * @return A stream of {@link SettingsForEntity} objects returned by the server.
     */
    @Nonnull
    public static Stream<SettingsForEntity> flattenEntitySettings(
            @Nonnull final Iterator<GetEntitySettingsResponse> iterator) {
        final Iterable<GetEntitySettingsResponse> respIt = () -> iterator;
        return StreamSupport.stream(respIt.spliterator(), false)
            .flatMap(resp -> resp.getSettingsList().stream());
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

    public static NumericSettingValue createNumericSettingValue(float value) {
        return NumericSettingValue.newBuilder()
            .setValue(value)
            .build();
    }

    public static BooleanSettingValue createBooleanSettingValue(boolean value) {
        return BooleanSettingValue.newBuilder()
            .setValue(value)
            .build();
    }

    public static StringSettingValue createStringSettingValue(String value) {
        return StringSettingValue.newBuilder()
            .setValue(value)
            .build();
    }

    public static EnumSettingValue createEnumSettingValue(String value) {
        return EnumSettingValue.newBuilder()
            .setValue(value)
            .build();
    }
}
