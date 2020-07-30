package com.vmturbo.topology.processor.util;

import java.util.EnumSet;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.Probe;
import com.vmturbo.common.protobuf.utils.ProbeFeature;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;

/**
 * Util class works with probe features.
 */
public class ProbeFeaturesUtil {

    private ProbeFeaturesUtil() {}

    /**
     * Return features provided by probe.
     *
     * @param probeInfo the info of certain {@link Probe}
     * @return the list of features provided by the probe
     */
    public static Set<ProbeFeature> getFeaturesProvidedByProbe(@Nonnull ProbeInfo probeInfo) {
        final Set<ProbeFeature> providedFeatures = EnumSet.noneOf(ProbeFeature.class);
        if (probeInfo.hasFullRediscoveryIntervalSeconds()
                || probeInfo.hasIncrementalRediscoveryIntervalSeconds()
                || probeInfo.hasPerformanceRediscoveryIntervalSeconds()) {
            providedFeatures.add(ProbeFeature.DISCOVERY);
        }
        if (probeInfo.hasDiscoversSupplyChain()) {
            providedFeatures.add(ProbeFeature.SUPPLY_CHAIN);
        }
        if (!probeInfo.getActionPolicyList().isEmpty()) {
            providedFeatures.add(ProbeFeature.ACTION_EXECUTION);
        }
        if (probeInfo.hasActionAudit()) {
            providedFeatures.add(ProbeFeature.ACTION_AUDIT);
        }
        if (probeInfo.hasActionApproval()) {
            providedFeatures.add(ProbeFeature.ACTION_APPROVAL);
        }
        if (probeInfo.hasPlanExport()) {
            providedFeatures.add(ProbeFeature.PLAN_EXPORT);
        }
        return providedFeatures;
    }
}
