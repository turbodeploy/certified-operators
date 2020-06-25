package com.vmturbo.topology.processor.util;

import java.util.concurrent.atomic.AtomicLong;

import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.PrimitiveValue;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.FakeRemoteMediation;

/**
 * Utility class to create some objects, suitable for tests.
 */
public class Probes {

    public static final ProbeInfo defaultProbe;

    /**
     * Probe that supports incremental discovery.
     */
    public static final ProbeInfo incrementalProbe;

    /**
     * Probe without any fields.
     */
    public static final ProbeInfo emptyProbe;
    public static final AccountDefEntry mandatoryField;
    public static final AtomicLong counter = new AtomicLong();

    private Probes() {}

    static {
        mandatoryField = AccountDefEntry.newBuilder()
                        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                                        .setName(FakeRemoteMediation.TGT_ID)
                                        .setDescription("This is the ID of the target")
                                        .setDisplayName("Target ID")
                                        .setPrimitiveValue(PrimitiveValue.STRING))
                        .build();
        emptyProbe = ProbeInfo.newBuilder().setProbeType("probe-type-" + counter.getAndIncrement())
                        .setProbeCategory("category")
                        .setUiProbeCategory("ui-category")
                        .addTargetIdentifierField(FakeRemoteMediation.TGT_ID)
                        .addAccountDefinition(mandatoryField).build();
        defaultProbe = ProbeInfo.newBuilder(emptyProbe)
                        .setProbeType("probe-type-" + counter.getAndIncrement())
                        .setUiProbeCategory("ui-probe-type-" + counter.getAndIncrement())
                        .build();
        incrementalProbe =
            ProbeInfo.newBuilder(defaultProbe).setIncrementalRediscoveryIntervalSeconds(10).build();
    }

    /**
     * Method creates new ProbeInfo builder with unique category and type.
     * Builder is ready to be built.
     *
     * @return probe info builder.
     */
    public static ProbeInfo.Builder createEmptyProbe() {
        final long index = counter.getAndIncrement();
        return ProbeInfo.newBuilder(emptyProbe).setProbeType("probe-type-" + index)
                .setUiProbeCategory("probe-ui-category-" + index)
                .setProbeCategory("probe-category-" + index);
    }
}
