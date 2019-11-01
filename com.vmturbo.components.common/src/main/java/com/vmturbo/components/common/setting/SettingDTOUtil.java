package com.vmturbo.components.common.setting;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;


/**
 * Utilities for dealing with messages defined in {@link SettingProto} (Setting.proto).
 */
public final class SettingDTOUtil {

    private static final Logger logger = LogManager.getLogger();


    private SettingDTOUtil() {
    }

    /**
     * Arrange the settings represented by a set of {@link EntitySettingGroup} by entity
     * and setting spec name. This is useful if you need to look up settings for specific
     * entities.
     *
     * @param settingGroups A stream of {@link EntitySettingGroup} objects.
     * @return A map of (entity id) -> (setting spec name) -> (setting for that entity and spec name).
     */
    @Nonnull
    public static Map<Long, Map<String, Setting>> indexSettingsByEntity(
            @Nonnull final Stream<EntitySettingGroup> settingGroups) {
        final Map<Long, Map<String, Setting>> settingsByEntityAndName = new HashMap<>();
        final Map<Long, Set<String>> entitiesWithMultipleSettings = new HashMap<>();
        settingGroups.forEach(settingGroup -> {
            final Setting setting = settingGroup.getSetting();
            settingGroup.getEntityOidsList().forEach(entityId -> {
                final Map<String, Setting> settingsForEntity =
                    settingsByEntityAndName.computeIfAbsent(entityId, k -> new HashMap<>());
                final Setting existingSetting = settingsForEntity.putIfAbsent(
                    setting.getSettingSpecName(), setting);
                if (existingSetting != null) {
                    entitiesWithMultipleSettings.computeIfAbsent(entityId, k -> new HashSet<>())
                        .add(setting.getSettingSpecName());
                }
            });
        });

        if (!entitiesWithMultipleSettings.isEmpty()) {
            logger.warn("The following entities had some settings with multiple values." +
                " We always chose the first encountered value. {}", entitiesWithMultipleSettings);
        }

        return settingsByEntityAndName;
    }

    /**
     * Convert an iterator over {@link GetEntitySettingsResponse} objects (returned by a gRPC call)
     * to a stream of the contained {@link EntitySettingGroup} objects.
     *
     * @param settingsResponseIterator The iterator returned by the gRPC call.
     * @return A stream of {@link EntitySettingGroup} objects returned by the server.
     */
    @Nonnull
    public static Stream<EntitySettingGroup> flattenEntitySettings(
            @Nonnull final Iterator<GetEntitySettingsResponse> settingsResponseIterator) {
        final Iterable<GetEntitySettingsResponse> it = () -> settingsResponseIterator;
        return StreamSupport.stream(it.spliterator(), false)
            .flatMap(resp -> resp.getSettingGroupList().stream());
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
     * Has a setting the default value for its type.
     *
     * @param setting Setting to investigate.
     * @return true if the setting has the default value for its type.
     */
    public static boolean isDefaultValueSetting(Setting setting) {
        if (SettingProto.Setting.ValueCase.VALUE_NOT_SET.equals(setting.getValueCase())) {
            return false;
        }

        Setting settingDefault = setting.getDefaultInstanceForType();

        switch (setting.getValueCase()) {
        case BOOLEAN_SETTING_VALUE:
            return setting.getBooleanSettingValue().getValue()
                            == settingDefault.getBooleanSettingValue().getValue();
        case NUMERIC_SETTING_VALUE:
            return setting.getNumericSettingValue().getValue()
                            == settingDefault.getNumericSettingValue().getValue();
        case STRING_SETTING_VALUE:
            return Objects.equals(setting.getStringSettingValue().getValue(),
                            settingDefault.getStringSettingValue().getValue());
        case ENUM_SETTING_VALUE:
            return Objects.equals(setting.getEnumSettingValue().getValue(),
                            settingDefault.getEnumSettingValue().getValue());
        default:
            throw new IllegalArgumentException("Illegal setting value type: "
                + setting.getValueCase());
        }
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
