package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import io.grpc.Channel;

import com.vmturbo.api.dto.GroupApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingOptionApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.setting.SettingsPolicyApiDTO;
import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.api.enums.SettingScope;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.common.protobuf.SettingDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.ScopeCase;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.GsonPostProcessable;

/**
 * Responsible for mapping Settings-related XL objects to their API counterparts.
 */
public class SettingsMapper {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A constant indicating that a {@link SettingSpec} has no associated setting manager.
     */
    private static final String NO_MANAGER = "";

    private final SettingManagerMapping managerMapping;

    private final SettingSpecMapper settingSpecMapper;

    private final SettingPolicyMapper settingPolicyMapper;

    private final GroupServiceBlockingStub groupService;

    private final SettingServiceBlockingStub settingService;

    public SettingsMapper(@Nonnull final String specJsonFile,
                          @Nonnull final Channel groupComponentChannel) {
        this.managerMapping = loadManagerMappings(specJsonFile);
        this.settingSpecMapper = new DefaultSettingSpecMapper();
        this.settingPolicyMapper = new DefaultSettingPolicyMapper(this);
        this.groupService = GroupServiceGrpc.newBlockingStub(groupComponentChannel);
        this.settingService = SettingServiceGrpc.newBlockingStub(groupComponentChannel);
    }

    @VisibleForTesting
    SettingsMapper(@Nonnull final SettingManagerMapping managerMapping,
                   @Nonnull final SettingSpecMapper specMapper,
                   @Nonnull final SettingPolicyMapper policyMapper,
                   @Nonnull final Channel groupComponentChannel) {
        this.managerMapping = managerMapping;
        this.settingSpecMapper = specMapper;
        this.settingPolicyMapper = policyMapper;
        this.groupService = GroupServiceGrpc.newBlockingStub(groupComponentChannel);
        this.settingService = SettingServiceGrpc.newBlockingStub(groupComponentChannel);
    }

    @Nonnull
    private SettingManagerMapping loadManagerMappings(@Nonnull final String specJsonFile) {
        logger.info("Loading Setting Manager Mappings from {}...", specJsonFile);
        try (InputStream inputStream = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream(specJsonFile);
            InputStreamReader reader = new InputStreamReader(inputStream)) {
            final SettingManagerMapping mapping =
                    ComponentGsonFactory.createGson().fromJson(reader, SettingManagerMapping.class);
            logger.info("Successfully loaded Setting Manager Mappings.");
            return mapping;
        } catch (IOException e) {
            throw new RuntimeException("Unable to load setting manager mapping.", e);
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
                    final SettingManagerInfo info = managerMapping.getManagerInfo(entry.getKey())
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
        final Optional<SettingManagerInfo> infoOpt = managerMapping.getManagerInfo(desiredMgrId);
        return infoOpt.map(info -> createMgrDto(desiredMgrId, info, specs.stream()
                .filter(spec -> managerMapping.getManagerUuid(spec.getName())
                    .map(name -> name.equals(desiredMgrId))
                    .orElse(false))
                .collect(Collectors.toList())));

    }

    /**
     * Convert a {@link SettingsPolicyApiDTO} received as input to create a setting policy into
     * a matching {@link SettingPolicyInfo} that can be used inside XL.
     *
     * @param apiInputPolicy The {@link SettingsPolicyApiDTO} received from the user.
     * @return The {@link SettingPolicyInfo} representing the input DTO for internal XL
     *         communication.
     */
    @Nonnull
    public SettingPolicyInfo convertInputPolicy(@Nonnull final SettingsPolicyApiDTO apiInputPolicy)
            throws InvalidOperationException {
        final Map<String, SettingSpec> specsByName = new HashMap<>();
        if (apiInputPolicy.getSettingsManagers() != null) {
            final Set<String> involvedSettings = apiInputPolicy.getSettingsManagers().stream()
                    .filter(settingMgr -> settingMgr.getSettings() != null)
                    .flatMap(settingMgr -> settingMgr.getSettings().stream())
                    .map(SettingApiDTO::getUuid)
                    .collect(Collectors.toSet());

            if (!involvedSettings.isEmpty()) {
                settingService.searchSettingSpecs(SearchSettingSpecsRequest.newBuilder()
                        .addAllSettingSpecName(involvedSettings)
                        .build())
                        .forEachRemaining(spec -> specsByName.put(spec.getName(), spec));
            }

            // Technically this shouldn't happen because we only return settings we support
            // from the SettingsService.
            if (!specsByName.keySet().equals(involvedSettings)) {
                throw new InvalidOperationException("Attempted to create a settings policy with " +
                        "invalid specs: " + Sets.difference(involvedSettings, specsByName.keySet()));
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

        if (apiInputPolicy.getEntityType() == null) {
            // At the time of this writing (Oct 4, 2017) the UI does NOT set the entity type
            // in the input policy, because all settings in legacy apply to at most one entity type.
            // We will try to infer the entity type based on the specs.
            infoBuilder.setEntityType(inferEntityType(specsByName));
        } else {
            infoBuilder.setEntityType(ServiceEntityMapper.fromUIEntityType(
                    apiInputPolicy.getEntityType()));
        }

        infoBuilder.setEnabled(apiInputPolicy.getDisabled() == null || !apiInputPolicy.getDisabled());

        if (apiInputPolicy.getSettingsManagers() != null) {
            apiInputPolicy.getSettingsManagers().stream()
                .flatMap(settingMgr -> settingMgr.getSettings().stream())
                .map(settingApiDto -> {
                    final Setting.Builder settingBuilder = Setting.newBuilder()
                            .setSettingSpecName(settingApiDto.getUuid());
                    final String value = settingApiDto.getValue();
                    // We expect the SettingSpec to be found.
                    final SettingSpec spec = specsByName.get(settingApiDto.getUuid());
                    if (spec == null) {
                        throw new IllegalArgumentException("Spec " + settingApiDto.getUuid() +
                                " not found in the specs given to the mapper.");
                    }
                    switch (spec.getSettingValueTypeCase()) {
                        case BOOLEAN_SETTING_VALUE_TYPE:
                            settingBuilder.setBooleanSettingValue(BooleanSettingValue.newBuilder()
                                    .setValue(Boolean.valueOf(value)));
                            break;
                        case NUMERIC_SETTING_VALUE_TYPE:
                            settingBuilder.setNumericSettingValue(NumericSettingValue.newBuilder()
                                    .setValue(Float.valueOf(value)));
                            break;
                        case STRING_SETTING_VALUE_TYPE:
                            settingBuilder.setStringSettingValue(StringSettingValue.newBuilder()
                                    .setValue(value));
                            break;
                        case ENUM_SETTING_VALUE_TYPE:
                            settingBuilder.setEnumSettingValue(EnumSettingValue.newBuilder()
                                    .setValue(value));
                            break;
                    }
                    return settingBuilder;
                }).forEach(infoBuilder::addSettings);
        }

        return infoBuilder.build();
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
                            group.getInfo().getName()));
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
    SettingManagerMapping getManagerMapping() {
        return managerMapping;
    }


    /**
     * Create a {@link SettingsManagerApiDTO} containing information about settings, but not their
     * values. This will be a "thicker" manager than the one created by
     * {@link DefaultSettingPolicyMapper#createValMgrDto(String, SettingManagerInfo, Collection)},
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
                                            @Nonnull final SettingManagerInfo info,
                                            @Nonnull final Collection<SettingSpec> specs) {
        final SettingsManagerApiDTO mgrApiDto = new SettingsManagerApiDTO();
        mgrApiDto.setUuid(mgrId);
        mgrApiDto.setDisplayName(info.getDisplayName());
        mgrApiDto.setCategory(info.getDefaultCategory());

        mgrApiDto.setSettings(specs.stream()
                .map(settingSpecMapper::settingSpecToApi)
                .filter(Optional::isPresent).map(Optional::get)
                .collect(Collectors.toList()));
        return mgrApiDto;
    }

    /**
     * Infer the entity type based on the list of specs a policy applies to. Extracted from
     * {@link SettingsMapper#convertInputPolicy(SettingsPolicyApiDTO)} for easier testing.
     * This is built on the assumption that, as in legacy, entity types can be inferred from the
     * settings. That assumption is not necessarily true in XL, but it will be true for the settings
     * we migrate from legacy.
     *
     * @param specsByName The list of specs to look at, arranged by name.
     * @return The entity type shared by the specs.
     * @throws IllegalArgumentException If it's impossible to narrow the specs down to exactly
     *                                  one entity type.
     */
    @VisibleForTesting
    int inferEntityType(@Nonnull final Map<String, SettingSpec> specsByName) {
        final Optional<Set<Integer>> entityTypesOpt =
                SettingDTOUtil.getOverlappingEntityTypes(specsByName.values());
        if (!entityTypesOpt.isPresent() || entityTypesOpt.get().size() != 1) {
            // This means that we couldn't narrow things down to exactly one entity type
            // from the provided settings. This is a violation of the assumption that entity
            // types can be inferred from the settings, so throw an exception!
            // If this becomes an issue, we could still try to infer the entity type from
            // the groups, but that shouldn't be necessary.
            throw new IllegalArgumentException("Cannot infer a single entity type from the " +
                    "setting specs: " + StringUtils.join(specsByName.keySet(), ", "));
        }
        // At this point, we expect exactly one entity type.
        return entityTypesOpt.get().iterator().next();
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

        DefaultSettingPolicyMapper(@Nonnull final SettingsMapper mapper) {
            this.mapper = mapper;
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

            final SettingManagerMapping managerMapping = mapper.getManagerMapping();

            // Do the actual settings mapping.
            final Map<String, List<Setting>> settingsByMgr = info.getSettingsList().stream()
                    .collect(Collectors.groupingBy(setting ->
                            managerMapping.getManagerUuid(setting.getSettingSpecName())
                                    .orElse(NO_MANAGER)));

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
                        final SettingManagerInfo mgrInfo = managerMapping.getManagerInfo(entry.getKey())
                                .orElseThrow(() -> new IllegalStateException("Manager ID " +
                                        entry.getKey() + " not found despite being in the mappings earlier."));
                        return createValMgrDto(entry.getKey(), mgrInfo, entry.getValue());
                    })
                    .collect(Collectors.toList()));

            return apiDto;
        }

        /**
         * Create a {@link SettingsManagerApiDTO} containing setting values to return to the API.
         * This will be a "thinner" manager - all the settings will only have the UUIDs and values.
         *
         * @param mgrId The ID of the setting manager.
         * @param mgrInfo The {@link SettingManagerInfo} for the manager.
         * @param settings The {@link Setting} objects containing setting values. All of these settings
         *                 should be "managed" by this manager, but this method does not check.
         * @return The {@link SettingsManagerApiDTO}.
         */
        @Nonnull
        @VisibleForTesting
        SettingsManagerApiDTO createValMgrDto(@Nonnull final String mgrId,
                                              @Nonnull final SettingManagerInfo mgrInfo,
                                              @Nonnull final Collection<Setting> settings) {
            final SettingsManagerApiDTO mgrApiDto = new SettingsManagerApiDTO();
            mgrApiDto.setUuid(mgrId);
            mgrApiDto.setDisplayName(mgrInfo.getDisplayName());
            mgrApiDto.setCategory(mgrInfo.getDefaultCategory());

            mgrApiDto.setSettings(settings.stream()
                    .map(setting -> {
                        final SettingApiDTO apiDto = new SettingApiDTO();
                        apiDto.setUuid(setting.getSettingSpecName());
                        switch (setting.getValueCase()) {
                            case BOOLEAN_SETTING_VALUE:
                                apiDto.setValue(Boolean.toString(setting.getBooleanSettingValue().getValue()));
                                break;
                            case NUMERIC_SETTING_VALUE:
                                apiDto.setValue(Float.toString(setting.getNumericSettingValue().getValue()));
                                break;
                            case STRING_SETTING_VALUE:
                                apiDto.setValue(setting.getStringSettingValue().getValue());
                                break;
                            case ENUM_SETTING_VALUE:
                                apiDto.setValue(setting.getEnumSettingValue().getValue());
                                break;
                        }
                        return apiDto;
                    })
                    .collect(Collectors.toList()));
            return mgrApiDto;
        }
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
                    apiDTO.setScope(null);
                    if (entityScope.getScopeCase().equals(ScopeCase.ENTITY_TYPE_SET)) {
                        // TODO (roman, Sept 12, 2017): Right now there are no settings that
                        // have multiple entity types, so we can just print a warning if we
                        // see one. In the future, either the UI API should change, or we should
                        // create a list of SettingApiDTOs when a SettingSpec contains multiple
                        // types.
                        final EntityTypeSet supportedTypes = entityScope.getEntityTypeSet();
                        if (!supportedTypes.getEntityTypeList().isEmpty()) {
                            if (supportedTypes.getEntityTypeCount() > 1) {
                                logger.warn("The spec {} supports multiple entity types: {}." +
                                                " Using only the first one.", settingSpec.getName(),
                                        supportedTypes.getEntityTypeList());
                            }
                            apiDTO.setEntityType(
                                ServiceEntityMapper.toUIEntityType(supportedTypes.getEntityType(0)));
                        }
                    }
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

    /**
     * The Java POJO representing the mappings from Setting Manager UUIDs, to the information about
     * that Setting Manager - most notably the settings it manages.
     * <p>
     * The UI/API has the concept of Setting Manager as an object that "manages" a group of settings.
     * This is inherited from legacy, where these various managers are EMF objects.
     * In XL we don't have setting managers. We just have {@link SettingSpec}s for all the available
     * settings. Instead of introducing the manager concept to the data model in XL, we fake it
     * at the API-component level. The reason we need to simulate it is because, at the time of
     * this writing (Sept 13, 2017), the UI has hard-coded expectations based on the setting
     * managers defined in the legacy OpsMgr. If that goes away, we could just have a single
     * "fake" XL manager that owns all the settings.
     * <p>
     * There is a JSON file that gets loaded when constructing the SettingsMapper. That file
     * should contain all the SettingManager -> Setting mappings, as well as auxiliary information
     * about each manager. These will need to be hard-coded (and kept up to date) to match
     * what's in the legacy OpsMgr.
     */
    @VisibleForTesting
    static class SettingManagerMapping implements GsonPostProcessable {

        /**
         * (manager uuid) -> Information about that manager.
         */
        private final Map<String, SettingManagerInfo> managersByUuid;

        /**
         * A map to quickly look up the manager for a particular setting name.
         * Explicitly marked as "transient" because it's not part of the GSON
         * serialization, and is initialized as part
         * of {@link SettingManagerMapping#postDeserialize()}.
         */
        private transient Map<String, String> settingToManager = new HashMap<>();

        /**
         * Default constructor intentionally private. GSON constructs via reflection.
         */
        private SettingManagerMapping() {
            managersByUuid = new HashMap<>();
        }

        /**
         * Get the name of the manager that "manages" a particular setting.
         *
         * @param specName The name of the setting.
         * @return An optional containing the name of the manager that manages this setting.
         *         An empty optional if there is no matching manager.
         */
        Optional<String> getManagerUuid(@Nonnull final String specName) {
            return Optional.ofNullable(settingToManager.get(specName));
        }

        /**
         * Get information about a manager by it's UUID.
         *
         * @param mgrUuid The UUID of the manager.
         * @return An optional containing the {@link SettingManagerInfo} for the manager.
         *         An empty optional if the UUID is not found.
         */
        Optional<SettingManagerInfo> getManagerInfo(@Nonnull final String mgrUuid) {
            return Optional.of(managersByUuid.get(mgrUuid));
        }

        /**
         * Initialize the index of (setting name) -> (mgr uuid) after GSON deserialization
         * of the {@link SettingManagerMapping#managersByUuid} map.
         */
        @Override
        public void postDeserialize() {
            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            managersByUuid.forEach((mgrName, mgrInfo) -> {
                mgrInfo.getSettings().forEach((settingName) -> {
                    // At the time of this writing (Sept 2017) settings names must be
                    // globally unique.
                    builder.put(settingName, mgrName);
                });
            });
            settingToManager = builder.build();
        }
    }

    /**
     * The information about a specific Setting Manager. See {@link SettingManagerMapping}.
     */
    @VisibleForTesting
    static class SettingManagerInfo {
        /**
         * The display name of the manager. It's not clear at the time of this writing that
         * the display name is explicitly used anywhere.
         */
        private final String displayName;

        /**
         * This represents the default category for all settings managed by this manager.
         * <p>
         * At the time of this writing the UI assigns this category to any setting managed by this
         * manager that does not have an explicit path
         * (i.e. {@link SettingApiDTO#getCategories()} returns null/empty).
         */
        private final String defaultCategory;

        /**
         * The settings managed by this setting manager.
         * This must exactly match the name of some {@link SettingSpec}
         * ({@link SettingSpec#getName}).
         */
        private final Set<String> settings;

        /**
         * Default constructor intentionally private. GSON constructs via reflection.
         */
        private SettingManagerInfo() {
            displayName = "";
            defaultCategory = "";
            settings = new HashSet<>();
        }

        /**
         * Explicit constructor for testing only.
         */
        @VisibleForTesting
        SettingManagerInfo(@Nonnull final String displayName,
                @Nonnull final String defaultCategory,
                @Nonnull final Set<String> settings) {
           this.displayName = displayName;
           this.defaultCategory = defaultCategory;
           this.settings = settings;
        }

        String getDisplayName() {
            return displayName;
        }

        String getDefaultCategory() {
            return defaultCategory;
        }

        Set<String> getSettings() {
            return Collections.unmodifiableSet(settings);
        }
    }

}
