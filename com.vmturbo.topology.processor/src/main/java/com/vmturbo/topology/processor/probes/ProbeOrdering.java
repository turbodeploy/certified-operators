package com.vmturbo.topology.processor.probes;

import java.util.Comparator;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore.ProbeStitchingOperation;

/**
 * Interface implemented by classes that impose an ordering on probes for stitching purposes.
 * The interface is a Comparator of probe IDs for purposes of imposing an order on probe stitching.
 * It also returns a set of probe categories or types for a probe ID which indicates categories or
 * types of probes that have a dependency with a probe of the given ID and therefore may be used as
 * the scope for stitching with that probe.
 */
public interface ProbeOrdering extends Comparator<ProbeStitchingOperation>{

    /**
     * Return the set of categories that a probe should stitch with.
     *
     * @param probeInfo the {@link ProbeInfo} of the probe.
     * @return Set of Strings with the categories of probe data that the identified probe should
     * stitch with.
     */
    Set<ProbeCategory> getCategoriesForProbeToStitchWith(@Nonnull ProbeInfo probeInfo);

    /**
     * Return the set of probe types that a probe should stitch with.
     *
     * @param probeInfo the {@link ProbeInfo} of the probe.
     * @return Set of Strings with the types of probe data that the identified probe should
     * stitch with.
     */
    Set<SDKProbeType> getTypesForProbeToStitchWith(@Nonnull ProbeInfo probeInfo);

}
