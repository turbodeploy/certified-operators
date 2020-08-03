package com.vmturbo.cost.component.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.ResolverPricing;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.PricingDataIdentifier;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbException;

/**
 * A class for resolving the mapping of business accounts to account pricing data.
 */
public class LocalCostPricingResolver extends ResolverPricing {
    private static final Logger logger = LogManager.getLogger();

    // These are limits of price table data we want to see in the log (for INFO and DEBUG channels).
    private static final int PFX_PRICETAB_DEBUG_LIM = 12000;
    private static final int PFX_PRICETAB_INFO_LIM = 1200;

    // Quality log variables. Do _not_ remove, alter or use for program control.
    private static final Cache<String, String> priceTableCache = CacheBuilder.newBuilder()
            .expireAfterWrite(2, TimeUnit.HOURS).concurrencyLevel(5).maximumSize(800).build();

    private final BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore;

    private final PriceTableStore priceTableStore;

    private final DiscountStore discountStore;

    /**
     * Constructor for the local cost pricing resolver.
     *
     * @param priceTableStore The price table store.
     * @param businessAccountPriceTableKeyStore The business account price table key store.
     * @param identityProvider The identity provider.
     * @param discountStore The discount store.
     * @param discountApplicatorFactory The discount applicator factory.
     * @param topologyEntityInfoExtractor The topology entity info extractor.
     */
    public LocalCostPricingResolver(@Nonnull final PriceTableStore priceTableStore,
                                    @Nonnull final BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore,
                                    @Nonnull IdentityProvider identityProvider,
                                    @Nonnull DiscountStore discountStore,
                                    @Nonnull DiscountApplicatorFactory<TopologyEntityDTO>
                                            discountApplicatorFactory,
                                    @Nonnull TopologyEntityInfoExtractor topologyEntityInfoExtractor) {
        super(discountApplicatorFactory, topologyEntityInfoExtractor);
        this.businessAccountPriceTableKeyStore = Objects.requireNonNull(businessAccountPriceTableKeyStore);
        this.priceTableStore = priceTableStore;
        this.discountStore = discountStore;
    }

    @Override
    public Map<Long, AccountPricingData<TopologyEntityDTO>> getAccountPricingDataByBusinessAccount(
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopo)
            throws CloudCostDataRetrievalException {
        // A map of the PricingDataIdentifier to the Account Pricing data.
        // This map helps reusing AccountPricingData which often is identical for many BAs.
        Map<PricingDataIdentifier, AccountPricingData<TopologyEntityDTO>>
                accountPricingDataMapByPricingDataIdentifier = new HashMap<>();

        Map<Long, AccountPricingData<TopologyEntityDTO>> accountPricingDataByBusinessAccountOid =
                new HashMap<>();
        Set<Long> baOids = cloudTopo.getAllEntitiesOfType(EntityType.BUSINESS_ACCOUNT_VALUE).stream()
                .map(TopologyEntityDTO::getOid).collect(Collectors.toSet());

        final Map<Long, Long> priceTableKeyOidByBusinessAccountOid = businessAccountPriceTableKeyStore
                .fetchPriceTableKeyOidsByBusinessAccount(baOids);
        logger.info("priceTableKeyByBusinessAccount = {}", priceTableKeyOidByBusinessAccountOid);

        Collection<Long> prTabKeyOids = priceTableKeyOidByBusinessAccountOid.values();
        final Map<Long, PriceTable> priceTableByPriceTableKeyOid = priceTableStore.getPriceTables(
                prTabKeyOids);
        final Map<Long, PriceTableKey> priceTableKeyByPriceTableKeyOid = priceTableStore.getPriceTableKeys(
                prTabKeyOids);

        final Map<Long, Discount> discountsByAccount;
        try {
            discountsByAccount = discountStore.getAllDiscount().stream()
                    .collect(Collectors.toMap(Discount::getAssociatedAccountId, Function.identity()));
        } catch (DbException e) {
            throw new CloudCostDataRetrievalException(e);
        }

        Map<Long, PriceTable> priceTableByBusinessAccountOid = getPriceTableByBusinessAccountOid(
                priceTableKeyOidByBusinessAccountOid, priceTableByPriceTableKeyOid);
        for (Map.Entry<Long, PriceTable> entry : priceTableByBusinessAccountOid.entrySet()) {
            Long baOid = entry.getKey();
            PriceTable priceTable = entry.getValue();
            Long priceTableKeyOid = priceTableKeyOidByBusinessAccountOid.get(baOid);
            PriceTableKey prTabKey = priceTableKeyByPriceTableKeyOid.get(priceTableKeyOid);
            Map<PriceTableKey, Long> checksumByPrTabKey = priceTableStore.getChecksumByPriceTableKeys(
                    Collections.singletonList(prTabKey));

            logPriceTableProps(priceTable, baOid, priceTableKeyOid, prTabKey, checksumByPrTabKey.get(prTabKey));

            buildPricingDataByBizAccount(cloudTopo, accountPricingDataMapByPricingDataIdentifier, discountsByAccount,
                    accountPricingDataByBusinessAccountOid, baOid, priceTable, priceTableKeyOid);
        }

        return accountPricingDataByBusinessAccountOid;
    }

    private void logPriceTableProps(PriceTable priceTable, Long businessAccountOid, Long priceTableOid,
            PriceTableKey prTabKey, Long checksum) {
        logOnce(priceTableCache, String.valueOf(priceTable.hashCode()), "",
                () -> String.format("priceTable oid %d for BA %d - (key: %s; hash: %d; checksum: %d)\n%s",
                        priceTableOid, businessAccountOid, prTabKey, priceTable.hashCode(), checksum,
                        getPriceTabPfx(priceTable, PFX_PRICETAB_INFO_LIM, PFX_PRICETAB_DEBUG_LIM)), null);
    }

    private String getPriceTabPfx(PriceTable priceTable, int pfxInfoLen, int pfxDebugLen) {
        final String prTabStr = priceTable.toString();
        final int prTabSize = prTabStr.length();

        if (logger.isDebugEnabled()) {
            return prTabStr.substring(0, pfxDebugLen < prTabSize ? pfxDebugLen : prTabSize) + " ...";
        }
        return prTabStr.substring(0, pfxInfoLen < prTabSize ? pfxInfoLen : prTabSize) + " ...";
    }

    /**
     * Log and cache user message once per supplied key.
     *
     * @param logTrackingCache cache used to track log messages
     * @param key the key for the cache
     * @param val the value for the key to store in cache
     * @param msgSupplier function to format the user message
     * @param e exception to report with the message if any.
     */
    private void logOnce(Cache<String, String> logTrackingCache, String key, String val,
            Supplier<?> msgSupplier, Exception e) {
        String cachedVal = logTrackingCache.getIfPresent(key);

        if (cachedVal == null) {
            logTrackingCache.put(key, val);
            if (e == null) {
                logger.info(msgSupplier);
            } else {
                logger.error(msgSupplier, e);
            }
        }
    }
}
