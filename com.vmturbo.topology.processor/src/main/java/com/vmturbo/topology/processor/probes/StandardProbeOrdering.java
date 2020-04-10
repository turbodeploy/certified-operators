package com.vmturbo.topology.processor.probes;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore.ProbeStitchingOperation;

/**
 * ProbeOrdering that we use by default for determining the order of probe stitching operations
 * and scope of stitching operations based on probe ID.
 *
 * Note: this comparator imposes orderings that are inconsistent with equals.
 * This implies it is not safe to use in sorted collections.
 */
public class StandardProbeOrdering implements ProbeOrdering {

    private static final Logger logger = LogManager.getLogger();
    private final ProbeStitchingDependencyTracker stitchingDependencyTracker =
            ProbeStitchingDependencyTracker.getDefaultStitchingDependencyTracker();

    @Nullable
    private final ProbeStitchingOperationComparator comparator;

    /**
     * Construct a StandardProbeOrdering based on the passed in probeStore.
     *
     * @param probeStore The {@link ProbeStore} whose probes to impose a ProbeOrdering on.
     */
    public StandardProbeOrdering(@Nonnull final ProbeStore probeStore) {
        ProbeStitchingOperationComparator psoc = null;

        try {
            psoc = stitchingDependencyTracker.createOperationComparator(probeStore);
        } catch (ProbeException e) {
            logger.error("Unable to create stitching oepration comparator due to", e);
        }
        comparator = psoc;
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
        if (comparator != null) {
            return comparator.compare(op1, op2);
        } else {
            // We are unable to determine a sort order without a comparator. Be very noisy
            // about the problem.
            logger.error("No comparator for comparing stitching operations!");
            return 0;
        }
    }
}
