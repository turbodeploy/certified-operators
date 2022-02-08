package com.vmturbo.market.runner.reconfigure.vcpu;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure.SettingChange;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup.SettingPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Util class for tests.
 */
public class VcpuScalingReconfigureActionGeneratorTestUtils {
    protected TopologyEntityDTO makeVM(long oid, int currentCoresPerSocket, boolean changeable,
                             boolean stale) {
        return TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .setAnalysisSettings(AnalysisSettings.newBuilder().setReconfigurable(true))
                .setStale(stale)
                .setTypeSpecificInfo(
                        TypeSpecificInfo.newBuilder()
                                .setVirtualMachine(
                                        VirtualMachineInfo.newBuilder()
                                                .setCoresPerSocketRatio(currentCoresPerSocket)
                                                .setCoresPerSocketChangeable(changeable))
                ).build();
    }

    protected TopologyEntityDTO makeVM(long oid, int currentCoresPerSocket, int numCpu,
                    boolean changeable, Long pmId) {
        TopologyEntityDTO.Builder vmBuilder = TopologyEntityDTO.newBuilder()
                        .setOid(oid)
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEnvironmentType(EnvironmentType.ON_PREM)
                        .setAnalysisSettings(AnalysisSettings.newBuilder().setReconfigurable(true))
                        .setTypeSpecificInfo(
                                        TypeSpecificInfo.newBuilder()
                                                        .setVirtualMachine(
                                                                        VirtualMachineInfo.newBuilder()
                                                                                        .setCoresPerSocketRatio(currentCoresPerSocket)
                                                                                        .setCoresPerSocketChangeable(changeable)
                                                                                        .setNumCpus(numCpu))
                        );
        if (pmId != null) {
            vmBuilder.addCommoditiesBoughtFromProviders(
                            CommoditiesBoughtFromProvider.newBuilder()
                                            .setProviderId(pmId)
                                            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            );
        }
        return vmBuilder.build();
    }

    GetEntitySettingsResponse makeGetEntitySettingsResponse(List<Long> vms,
            long policyId, String enumValue, Float numericValue) {
        return GetEntitySettingsResponse.newBuilder()
                .addSettingGroup(
                        makeEntitySettingGroup(vms, policyId, enumValue, numericValue)
                ).build();
    }

    EntitySettingGroup.Builder makeEntitySettingGroup(List<Long> vms,
            long policyId, String enumValue, Float numericValue) {
        Setting.Builder setting = Setting.newBuilder();
        if (enumValue != null) {
            setting.setEnumSettingValue(EnumSettingValue.newBuilder().setValue(enumValue));
        }
        if (numericValue != null) {
            setting.setNumericSettingValue(NumericSettingValue.newBuilder().setValue(numericValue));
        }
        return EntitySettingGroup.newBuilder()
                .addAllEntityOids(vms)
                .addPolicyId(SettingPolicyId.newBuilder()
                        .setPolicyId(policyId))
                .setSetting(setting);
    }

    Action makeResizeAction(long targetId) {
        Action resizeAction = Action.newBuilder()
                .setInfo(
                        ActionInfo.newBuilder().setResize(
                                Resize.newBuilder().setTarget(
                                        ActionEntity.newBuilder()
                                                .setId(targetId)
                                                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                ).setCommodityType(
                                        CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE).build()
                                )
                        )
                ).setExplanation(Explanation.newBuilder())
                .setId(1L)
                .setDeprecatedImportance(-1.0d)
                .build();
        return resizeAction;
    }



    @Nonnull
    protected ActionDTO.Reconfigure.SettingChange getFirstSettingChangeOfTheFirstAction(@Nonnull List<Action> actions) {
        Assert.assertThat(actions.isEmpty(), CoreMatchers.is(false));
        final Reconfigure reconfigure = actions.get(0).getInfo().getReconfigure();
        Assert.assertThat(reconfigure.getSettingChangeCount(), CoreMatchers.is(1));
        return reconfigure.getSettingChange(0);
    }

    @Nonnull
    protected Map<EntityAttribute, SettingChange> getChangesOfTheFirstAction(
                    @Nonnull List<Action> actions) {
        Assert.assertThat(actions.isEmpty(), CoreMatchers.is(false));
        final Reconfigure reconfigure = actions.get(0).getInfo().getReconfigure();
        return reconfigure.getSettingChangeList().stream()
                        .collect(Collectors.toMap(SettingChange::getEntityAttribute,
                                        Function.identity()));
    }

    /**
     * Creates PM instance with specified oid and specific number of sockets.
     *
     * @param oid of the desired PM entity.
     * @param socketsNum number of sockets for the PM.
     * @return return {@link TopologyEntityDTO} that represents PM instance.
     */
    @Nonnull
    protected static TopologyEntityDTO makePM(long oid, int socketsNum) {
        final PhysicalMachineInfo.Builder pmInfoBuilder =
                        PhysicalMachineInfo.newBuilder().setNumCpuSockets(socketsNum)
                                        .setNumCpuThreads(socketsNum * 2);
        return TopologyEntityDTO.newBuilder().setOid(oid)
                        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .setEnvironmentType(EnvironmentType.ON_PREM).setTypeSpecificInfo(
                                        TypeSpecificInfo.newBuilder()
                                                        .setPhysicalMachine(pmInfoBuilder)).build();
    }
}
