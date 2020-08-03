package com.vmturbo.cost.component.identity;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.identity.attributes.AttributeExtractor;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.attributes.SimpleMatchingAttributes;
import com.vmturbo.identity.attributes.SimpleMatchingAttributes.Builder;

/**
 * Extract the matching com.vmturbo.identity.attributes from a PriceTable.PriceTableKey.
 *
 * <p>PriceTableKey is a map keys used to uniquely identify a PriceTable.
 **/
public class PriceTableKeyExtractor implements AttributeExtractor<PriceTableKey> {
    /**
     * String representing the Price Table key identifiers.
     */
    public static final String PRICE_TABLE_KEY_IDENTIFIERS = "price_table_key_identifiers";
    private static final String SERVICE_PROVIDER_OID = "service_provider_oid";

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public IdentityMatchingAttributes extractAttributes(@Nonnull PriceTableKey priceTableKey) {
        Map<String, String> probeKeyMaterialMap = Maps.newHashMap(priceTableKey.getProbeKeyMaterialMap());
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        //adding pricing group as an identifier
        probeKeyMaterialMap.put(SERVICE_PROVIDER_OID, Long.toString(priceTableKey.getServiceProviderId()));
        return SimpleMatchingAttributes.newBuilder()
                .addAttribute(PRICE_TABLE_KEY_IDENTIFIERS, gson.toJson(probeKeyMaterialMap))
                .build();
    }

    /**
     * Construct a {@link SimpleMatchingAttributes} object representing this priceTableKeyOID DB table row.
     *
     * @param priceTableKey This includes the unique id and PriceTableKey.
     *                      of the priceTableKey.
     * @return a new {@link SimpleMatchingAttributes} initialized with the priceTableKey.
     */
    @VisibleForTesting
    @Nonnull
    public static IdentityMatchingAttributes getMatchingAttributes(String priceTableKey) {
        return new Builder()
                .addAttribute(PRICE_TABLE_KEY_IDENTIFIERS,
                        priceTableKey)
                .build();
    }
}
