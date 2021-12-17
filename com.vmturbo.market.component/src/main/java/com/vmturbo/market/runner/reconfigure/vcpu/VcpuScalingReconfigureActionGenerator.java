package com.vmturbo.market.runner.reconfigure.vcpu;

import static com.vmturbo.market.topology.conversions.MarketAnalysisUtils.EPSILON;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure.SettingChange;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup.SettingPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.market.runner.reconfigure.ExternalReconfigureActionGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Abstract class for vcpu scaling reconfigure generators.
 */
abstract class VcpuScalingReconfigureActionGenerator extends ExternalReconfigureActionGenerator {
    abstract List<Action> generateActions(
            @Nonnull SettingPolicyServiceBlockingStub settingPolicyService,
            @Nonnull Map<Long, TopologyEntityDTO> topologyEntities, Collection<Long> sourceVMs);

    protected List<Action> execute(@Nonnull SettingPolicyServiceBlockingStub settingPolicyService,
            @Nonnull Map<Long, TopologyEntityDTO> topologyEntities,
            @Nonnull Collection<Action> existingActions) {
        return generateActions(settingPolicyService, topologyEntities,
                findReconfigurableVMs(topologyEntities, existingActions));
    }

    /**
     * Find all VMs and their existing reason settings while retrieving given setting.
     *
     * @param settingPolicyService policy service
     * @param sourceEntities All entities we want to retrieve settings for.
     * @param vmIdToReasonSettings A map contains VMs to the existing policies on these VMs.
     * @param settingToRetrieve The setting we retrieve from policy service.
     * @param filter The criteria to accept VMs. (eg, get VMs with SocketPolicy ==
     *         UserSpecified).
     * @return VM Ids to existing reason settings. The VMs are filtered by given setting name and
     *         criteria.
     */
    protected static Map<Long, Set<Long>> findEntities2ReasonSettingsWithGivenSetting(
            @Nonnull SettingPolicyServiceBlockingStub settingPolicyService,
            @Nonnull Collection<Long> sourceEntities,
            @Nonnull Map<Long, Set<Long>> vmIdToReasonSettings,
            @Nonnull EntitySettingSpecs settingToRetrieve, @Nonnull Predicate<Setting> filter) {

        Stream<EntitySettingGroup> coreSocketRatioModeSettings = getSettings(settingPolicyService,
                sourceEntities, settingToRetrieve);
        // entityId -> List<policyIds>
        Map<Long, Set<Long>> result = new HashMap<>();
        coreSocketRatioModeSettings.filter(EntitySettingGroup::hasSetting).forEach(
                entitySettingGroup -> {
                    Setting setting = entitySettingGroup.getSetting();
                    if (filter.test(setting)) {
                        for (long entityOid : entitySettingGroup.getEntityOidsList()) {
                            if (!sourceEntities.contains(entityOid)) {
                                continue;
                            }
                            Set<Long> reasonPolicies = vmIdToReasonSettings.computeIfAbsent(
                                    entityOid, oid -> new HashSet<>());
                            for (SettingPolicyId policyId : entitySettingGroup.getPolicyIdList()) {
                                reasonPolicies.add(policyId.getPolicyId());
                                result.put(entityOid, reasonPolicies);
                            }
                        }
                    }
                });
        return result;
    }

    /**
     * Get given settings from entities from policy service.
     *
     * @param settingPolicyService The policy service retrieve settings from
     * @param oids The entities to get settings for.
     * @param setting The settings to retrieve
     * @return A stream of retrieved entitySettingGroup, this is defined in policy setting service.
     */
    protected static Stream<EntitySettingGroup> getSettings(
            SettingPolicyServiceBlockingStub settingPolicyService, Collection<Long> oids,
            EntitySettingSpecs setting) {
        EntitySettingFilter.Builder entitySettingFilter =
                EntitySettingFilter.newBuilder().addAllEntities(oids).addSettingName(
                        setting.getSettingName());

        GetEntitySettingsRequest request = GetEntitySettingsRequest.newBuilder().setSettingFilter(
                entitySettingFilter).setIncludeSettingPolicies(true).build();

        return SettingDTOUtil.flattenEntitySettings(
                settingPolicyService.getEntitySettings(request));
    }

    /**
     * Generate a reconfigure action that contain an entity attribute and current and new values.
     *
     * @param entity The entity that has this action
     * @param currentValue Current value.
     * @param newValue New value.
     * @param reasonSettings The policies that triggered this action.
     * @param attribute The entityAttribute this reconfigure action wants to change.
     * @return Reconfigure action.
     */
    protected static Action generateReconfigureActionWithEntityAttribute(TopologyEntityDTO entity,
            float currentValue, float newValue, Set<Long> reasonSettings,
            EntityAttribute attribute) {

        Reconfigure.Builder reconfigure = Reconfigure.newBuilder().setTarget(
                ActionEntity.newBuilder()
                        .setId(entity.getOid())
                        .setType(entity.getEntityType())
                        .setEnvironmentType(entity.getEnvironmentType())).setSettingChange(
                SettingChange.newBuilder()
                        .setCurrentValue(currentValue)
                        .setNewValue(newValue)
                        .setEntityAttribute(attribute));

        final Action.Builder action = Action.newBuilder()
                .setId(IdentityGenerator.next())
                .setExplanation(Explanation.newBuilder()
                        .setReconfigure(ReconfigureExplanation.newBuilder()
                                .addAllReasonSettings(reasonSettings)))
                .setInfo(ActionInfo.newBuilder().setReconfigure(reconfigure))
                .setDeprecatedImportance(-1.0d)
                //Todo: mark true when probe execution available.
                .setExecutable(false)
                .setSupportingLevel(SupportLevel.SUPPORTED)
                .setDisruptive(true);
        return action.build();
    }

    /**
     * Generate reconfigure actions for all entities whose current attribute values are different
     * from what policy specifies.
     *
     * @param settingPolicyService Policy service
     * @param desiredSetting The setting we want to compare, must be numeric.
     * @param entitiesToPolicyIds Entities to the reason policy ids.
     * @param topologyEntities Topology entities
     * @param getCurrentValue Get current attribute value from entity.
     * @param attribute The attribute to change within this action.
     * @return Reconfigure actions for VMs whose current cores per socket ratio is different from
     *         user specified.
     */
    protected static List<Action> generateActionsForEntitiesWithUndesiredNumericValue(
            SettingPolicyServiceBlockingStub settingPolicyService,
            @Nonnull EntitySettingSpecs desiredSetting,
            @Nonnull Map<Long, Set<Long>> entitiesToPolicyIds,
            @Nonnull Map<Long, TopologyEntityDTO> topologyEntities,
            @Nonnull Function<TopologyEntityDTO, Float> getCurrentValue,
            @Nonnull EntityAttribute attribute) {

        List<Action> result = new ArrayList<>();

        Stream<EntitySettingGroup> settings = getSettings(settingPolicyService,
                entitiesToPolicyIds.keySet(), desiredSetting);

        settings.filter(EntitySettingGroup::hasSetting).forEach(entitySettingGroup -> {
            SettingProto.Setting setting = entitySettingGroup.getSetting();
            if (setting.hasNumericSettingValue()) {
                float desiredValue = setting.getNumericSettingValue().getValue();
                if (desiredValue == 0) {
                    //vcpu scaling settings shouldn't have 0 as value
                    return;
                }
                for (long entityOid : entitySettingGroup.getEntityOidsList()) {
                    TopologyEntityDTO entity = topologyEntities.get(entityOid);
                    if (!entitiesToPolicyIds.containsKey(entityOid)) {
                        continue;
                    }
                    //If VM's current attribute is different from what the setting specifies, generate actions.
                    float currentValue = getCurrentValue.apply(entity);
                    if (Math.abs(currentValue - desiredValue) > EPSILON) {
                        //The reason policies contain both the policies from previous filtering
                        //and the policy in this method.
                        Set<Long> reasonPolicies = entitiesToPolicyIds.get(entityOid);
                        for (SettingPolicyId policyId : entitySettingGroup.getPolicyIdList()) {
                            reasonPolicies.add(policyId.getPolicyId());
                        }
                        Action reconfigureAction = generateReconfigureActionWithEntityAttribute(
                                entity, currentValue, desiredValue, reasonPolicies, attribute);
                        result.add(reconfigureAction);
                    }
                }
            }
        });
        return result;
    }

    /**
     * Find VMs with cores per socket > 0 and changeable from topology.
     *
     * @param topologyEntities The topology
     * @param existingActions Actions already generated.
     * @return VMs whose cores per socket are > 0 and changeable.
     */
    protected static Collection<Long> findReconfigurableVMs(
            @Nonnull Map<Long, TopologyEntityDTO> topologyEntities,
            Collection<Action> existingActions) {
        //If there are existing resize actions for this VM, we don't need to generate vcpu reconfigure actions,
        // as the change will be executable in resize actions.
        Set<Long> vcpuResizedVMs = existingActions.stream()
                .filter(Action::hasInfo)
                .map(Action::getInfo)
                .filter(ActionInfo::hasResize)
                .map(ActionInfo::getResize)
                .filter(resize -> resize.getCommodityType().getType() == CommodityType.VCPU_VALUE)
                .map(resize -> resize.getTarget().getId())
                .collect(Collectors.toSet());

        Stream<Long> changeableVMs = topologyEntities.values().stream()
                .filter(entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .filter(vm -> vm.getTypeSpecificInfo().hasVirtualMachine()
                        && vm.getTypeSpecificInfo().getVirtualMachine().getCoresPerSocketRatio() > 0
                        && vm.getTypeSpecificInfo().getVirtualMachine().getCoresPerSocketChangeable())
                .filter(vm -> vm.getAnalysisSettings().getReconfigurable())
                .map(TopologyEntityDTO::getOid);
        return changeableVMs.filter(vm -> !vcpuResizedVMs.contains(vm)).collect(Collectors.toList());
    }
}
