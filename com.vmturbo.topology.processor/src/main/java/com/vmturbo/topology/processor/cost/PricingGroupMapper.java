package com.vmturbo.topology.processor.cost;

import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * This is a utility class used to derive a pricing group (used during price table resolution) from
 * the discovering probe type of the pricing information. For example, the pricing information for
 * AWS business accounts will be discovered by {@link SDKProbeType#AWS}. The price table will be discovered
 * by {@link SDKProbeType#AWS_COST}. Both the price table and account will be in a pricing group of
 * "AWS". While the pricing group is currently defined by the provider, groups do not necessarily
 * need to be broken down this way.
 */
public class PricingGroupMapper {

    private static final String AWS_PRICING_GROUP_ID = "AWS";

    private static final String AZURE_PRICING_GROUP_ID = "Azure";

    private static final String GCP_PRICING_GROUP_ID = "GCP";

    private static final Map<SDKProbeType, String> PRICING_GROUP_BY_PROBE_TYPE = ImmutableMap
            .<SDKProbeType, String>builder()
            .put(SDKProbeType.AWS, AWS_PRICING_GROUP_ID)
            .put(SDKProbeType.AWS_BILLING, AWS_PRICING_GROUP_ID)
            .put(SDKProbeType.AWS_LAMBDA, AWS_PRICING_GROUP_ID)
            .put(SDKProbeType.AWS_COST, AWS_PRICING_GROUP_ID)
            .put(SDKProbeType.AZURE, AZURE_PRICING_GROUP_ID)
            .put(SDKProbeType.AZURE_EA, AZURE_PRICING_GROUP_ID)
            .put(SDKProbeType.AZURE_SERVICE_PRINCIPAL, AZURE_PRICING_GROUP_ID)
            .put(SDKProbeType.AZURE_STORAGE_BROWSE, AZURE_PRICING_GROUP_ID)
            .put(SDKProbeType.AZURE_COST, AZURE_PRICING_GROUP_ID)
            .put(SDKProbeType.GCP, GCP_PRICING_GROUP_ID)
            .put(SDKProbeType.GCP_COST, GCP_PRICING_GROUP_ID)
            .build();

    /**
     * This is a static utility class.
     */
    private PricingGroupMapper() {}


    /**
     * Resolves the pricing group (used in checking {@link com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey}
     * equality), from the {@code probeType}.
     * @param probeType The target {@link SDKProbeType}
     * @return The pricing group, or an empty {@link Optional} if a pricing group can not be resolved.
     */
    public static Optional<String> getPricingGroupForProbeType(@Nonnull SDKProbeType probeType) {
        return Optional.ofNullable(PRICING_GROUP_BY_PROBE_TYPE.get(probeType));
    }
}
