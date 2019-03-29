package com.vmturbo.topology.processor.probes;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore.ProbeStitchingOperation;

/**
 * ProbeOrdering that we use by default for determining the order of probe stitching operations
 * and scope of stitching operations based on probe ID.
 */
public class StandardProbeOrdering implements ProbeOrdering {

    private final ProbeStore probeStore;
    private final Logger logger = LogManager.getLogger();

    private static final ProbeStitchingDependencyTracker stitchingDependencyTracker =
            ProbeStitchingDependencyTracker.getDefaultStitchingDependencyTracker();
    /**
     * Construct a StandardProbeOrdering based on the passed in probeStore.
     *
     * @param probeStore The {@link ProbeStore} whose probes to impose a ProbeOrdering on.
     */
    public StandardProbeOrdering(@Nonnull final ProbeStore probeStore) {
        Objects.requireNonNull(probeStore);
        this.probeStore = probeStore;
    }

    @Override
    public Set<ProbeCategory> getCategoriesForProbeToStitchWith(@Nonnull final ProbeInfo probeInfo) {
        String probeCategory = probeInfo.getProbeCategory();
        if (probeCategory == null) {
            return Sets.newHashSet();
        }
        return stitchingDependencyTracker
                .getProbeCategoriesThatStitchBefore(ProbeCategory.create(probeCategory));
    }

    @Override
    public Set<SDKProbeType> getTypesForProbeToStitchWith(@Nonnull final ProbeInfo probeInfo) {
        String probeType = probeInfo.getProbeType();
        if (probeType == null) {
            return Sets.newHashSet();
        }
        return stitchingDependencyTracker
                .getProbeTypesThatStitchBefore(SDKProbeType.create(probeType));
    }

    @Override
    public int compare(final ProbeStitchingOperation op1, final ProbeStitchingOperation op2) {
        Long probeId1 = op1.probeId;
        Long probeId2 = op2.probeId;
        Objects.requireNonNull(probeId1);
        Objects.requireNonNull(probeId2);
        Optional<ProbeInfo> probe1Info = probeStore.getProbe(probeId1);
        Optional<ProbeInfo> probe2Info = probeStore.getProbe(probeId2);
        if (!probe1Info.isPresent()) {
            logger.error("Unrecognized probe ID {}", probeId1);
            return 0;
        }
        if (!probe2Info.isPresent()) {
            logger.error("Unrecognized probe ID {}", probeId2);
            return 0;
        }

        final ProbeCategory probe1Category =
                ProbeCategory.create(probe1Info.get().getProbeCategory());
        final ProbeCategory probe2Category =
                ProbeCategory.create(probe2Info.get().getProbeCategory());
        if (stitchingDependencyTracker.getProbeCategoriesThatStitchBefore(probe1Category)
                .contains(probe2Category)) {
            return 1;
        }
        if (stitchingDependencyTracker.getProbeCategoriesThatStitchBefore(probe2Category)
                .contains(probe1Category)) {
            return -1;
        }

        // if same ProbeCategories, check if there is ordering for diff probe type
        final SDKProbeType probe1Type = SDKProbeType.create(probe1Info.get().getProbeType());
        final SDKProbeType probe2Type = SDKProbeType.create(probe2Info.get().getProbeType());
        if (stitchingDependencyTracker.getProbeTypesThatStitchBefore(probe1Type)
                .contains(probe2Type)) {
            return 1;
        }
        if (stitchingDependencyTracker.getProbeTypesThatStitchBefore(probe2Type)
                .contains(probe1Type)) {
            return -1;
        }
        return 0;
    }
}
