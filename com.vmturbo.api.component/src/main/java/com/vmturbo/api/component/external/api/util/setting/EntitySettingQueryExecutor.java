package com.vmturbo.api.component.external.api.util.setting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingApiDTOPossibilities;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.setting.SettingActivePolicyApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.setting.SettingDTOUtil;

/**
 * Utility to query per-entity settings (for entities or groups), and the policies the settings
 * come from.
 */
public class EntitySettingQueryExecutor {

    private static final Logger logger = LogManager.getLogger();

    private final SettingPolicyServiceBlockingStub settingPolicyService;

    private final SettingServiceBlockingStub settingService;

    private final GroupExpander groupExpander;

    private final EntitySettingGroupMapper entitySettingGroupMapper;

    private final SettingsManagerMapping settingsManagerMapping;

    public EntitySettingQueryExecutor(final SettingPolicyServiceBlockingStub settingPolicyService,
                                      final SettingServiceBlockingStub settingService,
                                      final GroupExpander groupExpander,
                                      final SettingsMapper settingsMapper,
                                      final SettingsManagerMapping settingsManagerMapping) {
        this(settingPolicyService, settingService, groupExpander,
            new EntitySettingGroupMapper(settingsMapper), settingsManagerMapping);
    }

    public EntitySettingQueryExecutor(final SettingPolicyServiceBlockingStub settingPolicyService,
                                      final SettingServiceBlockingStub settingService,
                                      final GroupExpander groupExpander,
                                      final EntitySettingGroupMapper entitySettingGroupMapper,
                                      final SettingsManagerMapping settingsManagerMapping) {
        this.settingPolicyService = settingPolicyService;
        this.settingService = settingService;
        this.groupExpander = groupExpander;
        this.entitySettingGroupMapper = entitySettingGroupMapper;
        this.settingsManagerMapping = settingsManagerMapping;
    }

    @Nonnull
    private Map<String, SettingSpec> getSettingSpecs(@Nonnull final Set<String> specNames) {
        final Iterable<SettingSpec> specIt = () ->
            settingService.searchSettingSpecs(SearchSettingSpecsRequest.newBuilder()
                .addAllSettingSpecName(specNames)
                .build());

        return StreamSupport.stream(specIt.spliterator(), false)
            .collect(Collectors.toMap(SettingSpec::getName, Function.identity()));
    }

    /**
     * Get the entity settings associated with a particular scope.
     * This will return both the setting descriptions and the current values.
     *
     * @param scope The scope for the query - this can be a group or an individual entity.
     * @param includePolicyBreakdown Whether or not to include which policies the settings come from.
     *                  If this is false, and the scope is a group, we will only return
     *                  the "dominant" setting value for each setting active on the group. The
     *                  dominant value is the one that applies to the most entities.
     * @return The settings active on this scope, represented as {@link SettingsManagerApiDTO}s.
     */
    @Nonnull
    public List<SettingsManagerApiDTO> getEntitySettings(@Nonnull final ApiId scope,
                                                         final boolean includePolicyBreakdown) {
        final Optional<UIEntityType> type = scope.getScopeType();
        final Set<Long> oids;
        if (scope.isGroup()) {
            oids = groupExpander.expandOids(Collections.singleton(scope.oid()));
        } else {
            oids = Collections.singleton(scope.oid());
        }

        final GetEntitySettingsRequest request =
            GetEntitySettingsRequest.newBuilder()
                .setSettingFilter(EntitySettingFilter.newBuilder()
                    .addAllEntities(oids)
                    .build())
                .setIncludeSettingPolicies(includePolicyBreakdown)
                .build();

        // Get the settings active on the entities in the scope.
        final Map<String, List<EntitySettingGroup>> settingGroupsBySpecName =
            SettingDTOUtil.flattenEntitySettings(settingPolicyService.getEntitySettings(request))
                .collect(Collectors.groupingBy(grp -> grp.getSetting().getSettingSpecName()));

        final Map<String, SettingSpec> settingSpecs = getSettingSpecs(settingGroupsBySpecName.keySet());


        // Arrange the active settings by manager UUID.
        final Map<String, List<SettingApiDTO<String>>> settingsByMgrUuid = new HashMap<>();
        settingGroupsBySpecName.forEach((specName, settingGroups) -> {
            final Optional<String> mgrUuid = settingsManagerMapping.getManagerUuid(specName);
            if (!mgrUuid.isPresent()) {
                logger.warn("No manager found for spec: {}", specName);
                return;
            }

            final SettingSpec settingSpec = settingSpecs.get(specName);
            if (settingSpec == null) {
                logger.warn("No setting spec found for {}", specName);
                return;
            }

            final Optional<SettingApiDTO<String>> setting = entitySettingGroupMapper.toSettingApiDto(
                settingGroups, settingSpec, type, includePolicyBreakdown);
            if (!setting.isPresent()) {
                logger.warn("Failed to map {} setting groups for setting {} to API DTO.",
                    settingGroups.size(), specName);
                return;
            }

            final List<SettingApiDTO<String>> settingsForManager =
                settingsByMgrUuid.computeIfAbsent(mgrUuid.get(), k -> new ArrayList<>());
            settingsForManager.add(setting.get());
        });

        // Create the setting manager objects.
        final List<SettingsManagerApiDTO> retMgrs = settingsByMgrUuid.entrySet().stream()
            .map(entry -> {
                final Optional<SettingsManagerInfo> mgrInfo =
                    settingsManagerMapping.getManagerInfo(entry.getKey());
                if (!mgrInfo.isPresent()) {
                    logger.warn("Failed to find setting manager for mgr uuid: {}", entry.getKey());
                    return null;
                }
                final SettingsManagerApiDTO mgrDto = mgrInfo.get().newApiDTO(entry.getKey());
                mgrDto.setSettings(entry.getValue());
                return mgrDto;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        return retMgrs;
    }

    /**
     * Utility class to isolate the mapping logic from the fetching logic, mainly for unit testing.
     * Responsible for converting {@link EntitySettingGroup}s
     */
    static class EntitySettingGroupMapper {
        private final SettingsMapper settingsMapper;

        EntitySettingGroupMapper(final SettingsMapper settingsMapper) {
            this.settingsMapper = settingsMapper;
        }

        @Nonnull
        private Optional<SettingApiDTO<String>> dtoFromGroup(@Nonnull final EntitySettingGroup settingGroup,
                                                     final Optional<UIEntityType> type,
                                                     final SettingSpec settingSpec) {
            final SettingApiDTOPossibilities possibilities = settingsMapper.toSettingApiDto(
                settingGroup.getSetting(), settingSpec);
            return type.map(uiType -> possibilities.getSettingForEntityType(uiType.apiStr()))
                .orElseGet(() -> possibilities.getAll().stream().findFirst());
        }

        /**
         * Convert a list of entity setting groups for the same setting name to the {@link SettingApiDTO}
         * object to represent that setting in the API.
         *
         * @param groups A list of {@link EntitySettingGroup} objects. All the settings in the
         *               groups should have the same {@link Setting#getSettingSpecName()}. The
         *               various groups represent different values for the settings and,
         *               potentially, the different policies the settings got their values from.
         * @param settingSpec The {@link SettingSpec} for the setting shared by all the input
         *                    setting groups.
         * @param type The type of the scope we're getting the settings for. This is necessary to
         *             properly figure out which {@link SettingApiDTO} to map the setting spec
         *             to (since there may be multiple possibilities for the same spec but different
         *             entity types).
         * @param includePolicyBreakdown If true, populate the {@link SettingApiDTO#getActiveSettingsPolicies()}
         *                               field in the returned value using the policy information
         *                               in the input {@link EntitySettingGroup}s.
         * @return An optional containing the {@link SettingApiDTO} representing the setting.
         */
        @Nonnull
        public Optional<SettingApiDTO<String>> toSettingApiDto(@Nonnull final List<EntitySettingGroup> groups,
                                                       @Nonnull final SettingSpec settingSpec,
                                                       final Optional<UIEntityType> type,
                                                       final boolean includePolicyBreakdown) {
            Preconditions.checkArgument(!groups.isEmpty());

            // The dominant group is the one that applies to the most entities.
            // For example, if a group has 50 entities, and 45 of them have "suspend = MANUAL" while
            // 5 have "suspend = RECOMMEND", the one with "suspend = MANUAL" is the dominant group.
            final EntitySettingGroup dominantGroup = groups.stream()
                .max(Comparator.comparingInt(EntitySettingGroup::getEntityOidsCount))
                // Groups should be non-empty.
                .get();

            final Optional<SettingApiDTO<String>> dominantDtoOpt = dtoFromGroup(dominantGroup, type, settingSpec);
            return dominantDtoOpt.map(dominantDto -> {
                if (includePolicyBreakdown) {
                    // If "includePolicyBreakdown" is true we should have the policy ids in each
                    // setting group.
                    final List<SettingActivePolicyApiDTO> activePolicies;
                    if (groups.size() == 1 && groups.get(0).getPolicyIdList().stream()
                        .allMatch(settingPolicyId -> settingPolicyId.getType() == Type.DEFAULT)) {
                        // This is a special case - the only active policy is the default policy for
                        // this entity type. This doesn't count as an "active" policy for API/UI
                        // purposes.
                        activePolicies = Collections.emptyList();
                    } else {
                        activePolicies = groups.stream()
                            .map(settingGroup -> {
                                final Optional<SettingApiDTO<String>> dto = dtoFromGroup(settingGroup, type, settingSpec);
                                if (dto.isPresent()) {
                                    final SettingActivePolicyApiDTO settingActivePolicyApiDTO =
                                        new SettingActivePolicyApiDTO();

                                    // The SettingActivePolicyApiDTO has only one settingsPolicy.
                                    // That is why we take the first SettingPolicyId in the EntitySettingGroup.
                                    // If the API dto were to change, we could easily put multiple SettingsPolicies here.
                                    final BaseApiDTO policy = new BaseApiDTO();
                                    policy.setDisplayName(settingGroup.getPolicyId(0).getDisplayName());
                                    policy.setUuid(Long.toString(settingGroup.getPolicyId(0).getPolicyId()));
                                    settingActivePolicyApiDTO.setSettingsPolicy(policy);

                                    settingActivePolicyApiDTO.setNumEntities(settingGroup.getEntityOidsCount());
                                    settingActivePolicyApiDTO.setValue(dto.get().getValue());
                                    return settingActivePolicyApiDTO;
                                } else {
                                    logger.warn("Failed to get API DTO for setting: {}",
                                        settingGroup.getSetting());
                                    return null;
                                }
                            })
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
                    }
                    dominantDto.setActiveSettingsPolicies(activePolicies);

                    // In classic this is the group name - i.e. when looking at a single entity, this
                    // will tell you which group's policy got you here.
                    //
                    // Right now it doesn't look like anyone uses it, so we set it to the policy name
                    // instead. If we really need it, we could either make a follow-up call to get the
                    // group the policy applies to, or return the group name together with the
                    // policy ID. However, a policy can be applied to multiple groups, in which case
                    // we will need to know which of the groups an entity belongs to. This will
                    // require a reverse membership lookup, which is quite expensive.
                    dominantDto.setSourceGroupName(dominantGroup.getPolicyId(0).getDisplayName());
                    dominantDto.setSourceGroupUuid(Long.toString(dominantGroup.getPolicyId(0).getPolicyId()));
                }
                return dominantDto;
            });
        }
    }
}
