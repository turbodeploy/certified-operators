package com.vmturbo.market.runner.reconfigure;

import static com.vmturbo.market.topology.conversions.MarketAnalysisUtils.EPSILON;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.components.common.setting.VCPUScalingUnitsEnum;
import com.vmturbo.components.common.setting.VcpuScalingSocketsCoresPerSocketModeEnum;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Generate cores per socket change actions.
 */
class CoresPerSocketReconfigureActionGenerator extends ExternalReconfigureActionGenerator {

    @Override
    List<Action> execute(@Nonnull SettingPolicyServiceBlockingStub settingPolicyService,
            @Nonnull Map<Long, TopologyEntityDTO> topologyEntities) {
        Collection<Long> changeableVMs = topologyEntities.values().stream().filter(
                entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .filter(entity -> entity.getTypeSpecificInfo().hasVirtualMachine()
                        && entity.getTypeSpecificInfo().getVirtualMachine().getCoresPerSocketRatio() > 0
                        && entity.getTypeSpecificInfo().getVirtualMachine().getCoresPerSocketChangeable())
                .map(TopologyEntityDTO::getOid).collect(Collectors.toList());

        //3 Steps to generate actions:
        //1. Retrieve VMs who have VcpuScalingUnits and filter out those whose policy values are SOCKET.
        //2. Retrieve VcpuScaling_Socket_CoresPerSocketMode policy among the filtered VMs, and filter out those whose values are USER_SPECIFIED.
        //3. Retrieve VcpuScaling_Sockets_CoresPerSocketValue policy among the filtered VMs,
        // and generate actions for those whose policy values are different from their current cores per socket.

        Map<Long, Set<Long>> vmsWithScaleInSocketsToPolicyIds =
                findVMs2ReasonSettingsWithGivenSetting(settingPolicyService, changeableVMs,
                        new HashMap<>(), EntitySettingSpecs.VcpuScalingUnits,
                        setting -> setting.hasEnumSettingValue() && VCPUScalingUnitsEnum.SOCKETS
                            == VCPUScalingUnitsEnum.valueOf(setting.getEnumSettingValue().getValue()));

        Map<Long, Set<Long>> vmsWithUserSpecifiedCoresPerSocketToPolicyIds =
                findVMs2ReasonSettingsWithGivenSetting(settingPolicyService,
                        vmsWithScaleInSocketsToPolicyIds.keySet(),
                        vmsWithScaleInSocketsToPolicyIds,
                        EntitySettingSpecs.VcpuScaling_Sockets_CoresPerSocketMode,
                        setting -> setting.hasEnumSettingValue() && VcpuScalingSocketsCoresPerSocketModeEnum.USER_SPECIFIED
                                == VcpuScalingSocketsCoresPerSocketModeEnum.valueOf(setting.getEnumSettingValue().getValue()));
        return generateActionsForVMsWithDifferentCurrentCoresPerSocket(settingPolicyService,
                vmsWithUserSpecifiedCoresPerSocketToPolicyIds, topologyEntities);

    }

    private Stream<EntitySettingGroup> getSettings(
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
     * Find all VMs and their existing reason settings while retrieving given setting.
     *
     * @param settingPolicyService policy service
     * @param sourceVMs All VMs we want to retrieve settings for.
     * @param vmIdToReasonSettings A map contains VMs to the existing policies on these VMs.
     * @param settingToRetrieve The setting we retrieve from policy service.
     * @param filter The criteria to accept VMs. (eg, get VMs with SocketPolicy == UserSpecified).
     * @return VM Ids to existing reason settings. The VMs are filtered by given setting name and criteria.
     */
    private Map<Long, Set<Long>> findVMs2ReasonSettingsWithGivenSetting(
            @Nonnull SettingPolicyServiceBlockingStub settingPolicyService,
            @Nonnull Collection<Long> sourceVMs,
            @Nonnull Map<Long, Set<Long>> vmIdToReasonSettings,
            @Nonnull EntitySettingSpecs settingToRetrieve,
            @Nonnull Predicate<SettingProto.Setting> filter) {

        Stream<EntitySettingGroup> coreSocketRatioModeSettings = getSettings(settingPolicyService,
                sourceVMs, settingToRetrieve);
        // entityId -> List<policyIds>
        Map<Long, Set<Long>> result = new HashMap<>();
        coreSocketRatioModeSettings.filter(EntitySettingGroup::hasSetting).forEach(
                entitySettingGroup -> {
                    SettingProto.Setting setting = entitySettingGroup.getSetting();
                    if (filter.test(setting)) {
                        for (long entityOid : entitySettingGroup.getEntityOidsList()) {
                            if (!sourceVMs.contains(entityOid)) {
                                continue;
                            }
                            Set<Long> reasonPolicies = vmIdToReasonSettings
                                    .computeIfAbsent(entityOid, oid -> new HashSet<>());
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
     * Generate actions for all VMs whose current cores per socket is different from user_specified
     * value.
     *
     * @param settingPolicyService policy service
     * @param vmsWithUserSpecifiedValueToPolicyIds Ids of VMs with cores_per_socket_mode ==
     *         user_specified -> Ids of applied policies.
     * @param topologyEntities Topology entities
     * @return Reconfigure actions for VMs whose current cores per socket ratio is different from user specified.
     */
    private List<Action> generateActionsForVMsWithDifferentCurrentCoresPerSocket(
            SettingPolicyServiceBlockingStub settingPolicyService,
            @Nonnull Map<Long, Set<Long>> vmsWithUserSpecifiedValueToPolicyIds,
            @Nonnull Map<Long, TopologyEntityDTO> topologyEntities) {

        List<Action> result = new ArrayList<>();

        Stream<EntitySettingGroup> userSpecifiedCoresPerSocketSettings = getSettings(
                settingPolicyService, vmsWithUserSpecifiedValueToPolicyIds.keySet(),
                EntitySettingSpecs.VcpuScaling_Sockets_CoresPerSocketValue);

        userSpecifiedCoresPerSocketSettings.filter(EntitySettingGroup::hasSetting).forEach(
                entitySettingGroup -> {
                    SettingProto.Setting setting = entitySettingGroup.getSetting();
                    if (setting.hasNumericSettingValue()) {
                        float userSpecifiedCoresPerSocket =
                                setting.getNumericSettingValue().getValue();
                        if (userSpecifiedCoresPerSocket != 0) {
                            for (long entityOid : entitySettingGroup.getEntityOidsList()) {
                                TopologyEntityDTO entity = topologyEntities.get(entityOid);
                                if (!vmsWithUserSpecifiedValueToPolicyIds.containsKey(entityOid)) {
                                    continue;
                                }
                                //If VM's current core per socket is different from what user specifies in
                                //USER_SPECIFIED policy, generate actions.
                                int currentCoresPerSocket = entity.getTypeSpecificInfo()
                                        .getVirtualMachine()
                                        .getCoresPerSocketRatio();
                                if (Math.abs(currentCoresPerSocket - userSpecifiedCoresPerSocket) > EPSILON) {
                                    //The reason policies contain both the cores_per_socket_mode policy
                                    //and cores_per_socket_user_specified policy.
                                    Set<Long> reasonPolicies =
                                            vmsWithUserSpecifiedValueToPolicyIds.get(entityOid);
                                    for (SettingPolicyId policyId : entitySettingGroup.getPolicyIdList()) {
                                        reasonPolicies.add(policyId.getPolicyId());
                                    }
                                    Action reconfigureAction =
                                            generateReconfigureCoresPerSocketAction(entity,
                                                    currentCoresPerSocket,
                                                    userSpecifiedCoresPerSocket, reasonPolicies);
                                    result.add(reconfigureAction);
                                }
                            }
                        }
                    }
                });
        return result;
    }

    private Action generateReconfigureCoresPerSocketAction(TopologyEntityDTO entity,
            float currentValue, float newValue, Set<Long> reasonSettings) {

        ActionDTO.Reconfigure.Builder reconfigure = Reconfigure.newBuilder().setTarget(
                ActionEntity.newBuilder()
                        .setId(entity.getOid())
                        .setType(entity.getEntityType())
                        .setEnvironmentType(entity.getEnvironmentType())).setSettingChange(
                SettingChange.newBuilder()
                        .setCurrentValue(currentValue)
                        .setNewValue(newValue)
                        .setEntityAttribute(EntityAttribute.CORES_PER_SOCKET));

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
}
