package com.vmturbo.topology.processor.util;

import java.util.concurrent.atomic.AtomicLong;

import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.PrimitiveValue;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;

/**
 * Utility class to create some objects, suitable for tests.
 */
public class Probes {

    public static final ProbeInfo defaultProbe;
    /**
     * Probe without any fields.
     */
    public static final ProbeInfo emptyProbe;
    public static final AccountDefEntry mandatoryField;
    public static final String TARGET_ID = "targetId";
    public static final AtomicLong counter = new AtomicLong();

    private Probes() {}

    static {
        mandatoryField = AccountDefEntry.newBuilder()
                        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                                        .setName("name-" + counter.getAndIncrement())
                                        .setDescription("description-" + counter.getAndIncrement())
                                        .setDisplayName("display-name")
                                        .setPrimitiveValue(PrimitiveValue.STRING))
                        .build();
        emptyProbe = ProbeInfo.newBuilder().setProbeType("probe-type-" + counter.getAndIncrement())
                        .setProbeCategory("category").addTargetIdentifierField(TARGET_ID).build();
        defaultProbe = ProbeInfo.newBuilder(emptyProbe)
                        .setProbeType("probe-type-" + counter.getAndIncrement())
                        .addAccountDefinition(mandatoryField).build();
    }

    /**
     * Method creates new ProbeInfo builder with unique category and type. Builer is ready to be built.
     *
     * @return probe info builder.
     */
    public static ProbeInfo.Builder createEmptyProbe() {
        final long index = counter.getAndIncrement();
        return ProbeInfo.newBuilder(emptyProbe).setProbeType("probe-type-" + index)
                        .setProbeCategory("probe-category-" + index);
    }
}
