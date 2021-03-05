package com.vmturbo.stitching.poststitching;

import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * DisableActionForHxControllerVmAndDsOperation.
 * disable action from Hyperflex storage controller VM and DS in the Hyperv, vmm and VC environment.
 */
public class DisableActionForHxControllerVmAndDsOperation implements PostStitchingOperation {
    private static final Logger logger = LogManager.getLogger();
    /**
     * String for the Hyperflex storage control VM.
     * Currently Hyperflex only support two hypervisor: VC and HyperV.
     * Hyperflex storage control VM in VC name pattern: stCtlVM.*
     * Hyperflex storage control VM in HyperV name pattern: StCtlVM
     */
    private static final String ST_CTL_VM = "STCTLVM";

    /**
     * Based on this document, Hyperflex storage control VM in HyperV can also be: hxCtlVM.
     * https://www.cisco.com/c/en/us/td/docs/hyperconverged_systems/HyperFlex_HX_DataPlatformSoftware
     * /AdminGuide/4_0/b-hx-dp-administration-guide-for-hyper-v-4-0/b-hx-dp-administration-guide-for-hyper-v-4-0_chapter_0100.html
     */
    private static final String HX_CTL_VM = "HXCTLVM";

    /**
     * VC storage Controller VM uses storage with name pattern "SpringpathDS.*".
     */
    private static final String SPRING_PATH_DS = "SPRINGPATHDS";

    /**
     * HyperV storage Controller VM uses storage with name pattern ".*_F:".
     */
    private static final String UNDERSCORE_F_COLON = "_F:";


    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {

        // this operations applies to HyperV and VMM probes only
        final Set<String> probeTypes = ImmutableSet.of(
                SDKProbeType.VCENTER.getProbeType(),
                SDKProbeType.HYPERV.getProbeType(),
                SDKProbeType.VCD.getProbeType(),
                SDKProbeType.VMM.getProbeType());
        return stitchingScopeFactory.multiProbeEntityTypeScope(probeTypes, EntityType.VIRTUAL_MACHINE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
        @Nonnull Stream<TopologyEntity> entities,
        @Nonnull EntitySettingsCollection settingsCollection,
        @Nonnull EntityChangesBuilder<TopologyEntity> resultBuilder) {
            entities.forEach((TopologyEntity vm) -> {
                if (!isHxStorageControlVm(vm)) {
                    return;
                }

                final TopologyEntity ds = vm.getProviders().stream().filter(p ->
                        p.getEntityType() == EntityType.STORAGE_VALUE)
                        .findFirst().orElse(null);
                if (ds == null) {
                    return;
                }

                /*
                 * Controllable will be set to false for the VM and its provider
                 * storage when both VM and its provider Storage matches their specific
                 * name pattern of the Hyperflex controller VM and its datastore.
                 */

                if (isHxStorageControlVmDs(ds)) {
                    resultBuilder.queueUpdateEntityAlone(ds, entityToUpdate -> {
                        setControllableFalse(ds);
                    });
                    resultBuilder.queueUpdateEntityAlone(vm, entityToUpdate -> {
                        setControllableFalse(vm);
                    });
                }
            });
            return resultBuilder.build();
    }

    private static boolean isHxStorageControlVm(@Nonnull TopologyEntity vm) {
        final String vmName = vm.getDisplayName().toUpperCase();
        return vmName.startsWith(ST_CTL_VM) || vmName.contains(HX_CTL_VM);
    }

    private static boolean isHxStorageControlVmDs(@Nonnull TopologyEntity ds) {
        final String dsName = ds.getDisplayName().toUpperCase();
        return dsName.startsWith(SPRING_PATH_DS) || dsName.endsWith(UNDERSCORE_F_COLON);
    }

    private static void setControllableFalse(TopologyEntity entity) {
        logger.debug("Set Controllable flag to be false for entity {} of type {}",
                entity.getDisplayName(), entity.getJournalableEntityType().name());
        entity.getTopologyEntityDtoBuilder().getAnalysisSettingsBuilder()
                .setControllable(false);
    }
}
