package com.vmturbo.stitching.poststitching;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Ths post stitcher checks whether a VM has a hypervisor probe but doesn't have guest_os_process probe.
 * If so, this post stitcher will check the UseHypervisorMetricsForResizing setting, if the setting is false, mark VMEM of VM resizable false.
 */
public class UseHypervisorVmemForResizingPostStitchingOperation implements PostStitchingOperation {
    @Nonnull
    public StitchingScope<TopologyEntity> getScope(@Nonnull StitchingScope.StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        // Apply this calculation to all vms discovered by Hypervisor probes and have no guest_os_processes probe
        return stitchingScopeFactory.hasAndLacksProbeCategoryEntityTypeStitchingScope(
                Collections.singleton(ProbeCategory.HYPERVISOR),
                Collections.singleton(ProbeCategory.GUEST_OS_PROCESSES),
                CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(@Nonnull Stream<TopologyEntity> entities, @Nonnull EntitySettingsCollection settingsCollection, @Nonnull TopologicalChangelog.EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities.filter(vm -> !isHypervisorOnlyVMemResizableAllowed(vm, settingsCollection))
                .forEach(vm ->
                    resultBuilder.queueUpdateEntityAlone(vm,
                            entity -> entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList()
                                    .stream().filter(commodity -> CommonDTO.CommodityDTO.CommodityType.VMEM_VALUE == commodity.getCommodityType().getType())
                                    .forEach(vmem -> vmem.setIsResizeable(false)))

        );
        return resultBuilder.build();
    }

    private static boolean isHypervisorOnlyVMemResizableAllowed(@Nonnull TopologyEntity vm, @Nonnull EntitySettingsCollection settingsCollection) {
        final Optional<SettingProto.Setting> useHypervisorMetricsForResizing =
                settingsCollection.getEntitySetting(vm, EntitySettingSpecs.UseHypervisorMetricsForResizing);
        return useHypervisorMetricsForResizing.map(setting -> setting.getBooleanSettingValue().getValue()).orElse(true);

    }
}
