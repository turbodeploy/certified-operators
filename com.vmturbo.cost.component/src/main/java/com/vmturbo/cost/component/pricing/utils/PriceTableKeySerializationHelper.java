package com.vmturbo.cost.component.pricing.utils;

import javax.annotation.Nonnull;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey.Builder;

/**
 * Helper class used to transform protobuf to and from JSON.
 */

public class PriceTableKeySerializationHelper {

    private PriceTableKeySerializationHelper() {}

    /**
     * Serializes {@param priceTableKey} to json string which is written to DB.
     *
     * @param priceTableKey priceTableKey protobuf to be serialized.
     * @return json string.
     * @throws InvalidProtocolBufferException if invalid protobuf.
     */
    public static String serializeProbeKeyMaterial(@Nonnull final PriceTableKey priceTableKey)
            throws InvalidProtocolBufferException {
        return JsonFormat.printer().print(priceTableKey);
    }

    /**
     * Converts json string to {@param priceTableKey}.
     *
     * @param probeKeyMaterialString jsonstring from DB.
     * @return {@param priceTableKey} used to build rpc response.
     * @throws InvalidProtocolBufferException if unable to deserialize to {@link PriceTableKey}.
     */
    public static PriceTableKey deserializeProbeKeyMaterial(@Nonnull final String probeKeyMaterialString)
            throws InvalidProtocolBufferException {
        Builder priceTableKeyBuilder = PriceTableKey.newBuilder();
        JsonFormat.parser().merge(probeKeyMaterialString, priceTableKeyBuilder);
        return priceTableKeyBuilder.build();
    }
}
