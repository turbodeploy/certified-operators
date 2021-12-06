package com.vmturbo.market.runner.reconfigure.vcpu;

import java.util.List;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup.SettingPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Util class for tests.
 */
public class VcpuScalingReconfigureActionGeneratorTestUtils {
    TopologyEntityDTO makeVM(long oid, int currentCoresPerSocket, boolean changeable) {
        return TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .setAnalysisSettings(AnalysisSettings.newBuilder().setReconfigurable(true))
                .setTypeSpecificInfo(
                        TypeSpecificInfo.newBuilder()
                                .setVirtualMachine(
                                        VirtualMachineInfo.newBuilder()
                                                .setCoresPerSocketRatio(currentCoresPerSocket)
                                                .setCoresPerSocketChangeable(changeable))
                ).build();
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
                                )
                        )
                ).setExplanation(Explanation.newBuilder())
                .setId(1L)
                .setDeprecatedImportance(-1.0d)
                .build();
        return resizeAction;
    }
}
