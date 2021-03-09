package com.vmturbo.market.runner.cost;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.GetDiscountRequest;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.Pricing.GetAccountPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTablesRequest;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.integration.ResolverPricing;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.PricingDataIdentifier;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * The Market pricing resolver class.
 */
public class MarketPricingResolver extends ResolverPricing {
    private static final Logger logger = LogManager.getLogger();
    // Quality log variables. Do _not_ remove, alter or use for program control.
    private static final Cache<String, String> priceTableCache = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.HOURS).concurrencyLevel(5).maximumSize(800).build();
    private static final Cache<String, String> pricingByBACache = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.HOURS).concurrencyLevel(5).maximumSize(800).build();

    private final PricingServiceBlockingStub pricingServiceClient;

    private final CostServiceBlockingStub costServiceClient;

    /**
     * Constructor for the Market Pricing Resolver.
     *
     * @param pricingServiceClient      The pricing service client.
     * @param costServiceClient         The cost service client.
     * @param discountApplicatorFactory The discount applicator factory.
     * @param topologyEntityInfoExtractor The instance of the topology entity info extractor.
     */
    public MarketPricingResolver(@Nonnull PricingServiceBlockingStub pricingServiceClient,
                                 @Nonnull CostServiceBlockingStub costServiceClient,
                                 @Nonnull DiscountApplicatorFactory<TopologyEntityDTO>
                                         discountApplicatorFactory,
                                 @Nonnull TopologyEntityInfoExtractor topologyEntityInfoExtractor) {
        super(discountApplicatorFactory, topologyEntityInfoExtractor);
        this.pricingServiceClient = Objects.requireNonNull(pricingServiceClient);
        this.costServiceClient = Objects.requireNonNull(costServiceClient);
    }

    @Override
    public Map<Long, AccountPricingData<TopologyEntityDTO>> getAccountPricingDataByBusinessAccount(
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) throws CloudCostDataRetrievalException {

        // Get the set of business accounts in the topology.
        Set<Long> accountOids = cloudTopology.getAllEntitiesOfType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .stream()
                .map(TopologyEntityDTO::getOid)
                .collect(Collectors.toSet());

        // A map of the PricingDataIdentifier to the Account Pricing data.
        // This map helps reusing AccountPricingData which often is identical for many BAs.
        Map<PricingDataIdentifier, AccountPricingData<TopologyEntityDTO>>
                accountPricingDataMapByPricingDataIdentifier = new HashMap<>();

        // Get the discounts.
        final Map<Long, Discount> discountsByAccount = new HashMap<>();
        costServiceClient.getDiscounts(GetDiscountRequest.getDefaultInstance())
                .forEachRemaining(discount -> discountsByAccount.put(discount.getAssociatedAccountId(), discount));

        Map<Long, AccountPricingData<TopologyEntityDTO>> accountPricingDataByBusinessAccountOid
                = new HashMap<>();


        // Get a mapping of business account oid -> price table key oid.
        final Map<Long, Long> priceTableKeyOidByBusinessAccountOid = pricingServiceClient.getAccountPriceTable(
                GetAccountPriceTableRequest.newBuilder()
                        .addAllBusinessAccountOid(accountOids)
                        .build()).getBusinessAccountPriceTableKeyMap();
        logger.debug("priceTableKeyOidByBusinessAccountOid = {}", priceTableKeyOidByBusinessAccountOid);

        // Get a mapping of price table key oid to price table.
        final Map<Long, PriceTable> priceTableByPriceTableKeyOid = pricingServiceClient.getPriceTables(
                GetPriceTablesRequest.newBuilder().addAllOid(priceTableKeyOidByBusinessAccountOid.values())
                        .build()).getPriceTablesByOidMap();

        Map<Long, PriceTable> priceTableByBusinessAccountOid = getPriceTableByBusinessAccountOid(
                priceTableKeyOidByBusinessAccountOid, priceTableByPriceTableKeyOid);
        for (Map.Entry<Long, PriceTable> entry : priceTableByBusinessAccountOid.entrySet()) {
            Long baOid = entry.getKey();
            PriceTable priceTable = entry.getValue();
            Long priceTableOid = priceTableKeyOidByBusinessAccountOid.get(baOid);

            logPriceTableProps(priceTable, baOid, priceTableOid);

            buildPricingDataByBizAccount(cloudTopology, accountPricingDataMapByPricingDataIdentifier, discountsByAccount,
                    accountPricingDataByBusinessAccountOid, baOid, priceTable, priceTableOid);
        }

        logAccountPricingDataByBA(accountPricingDataByBusinessAccountOid);

        return accountPricingDataByBusinessAccountOid;
    }

    @Override
    public Map<Long, ReservedInstancePriceTable> getRIPriceTablesByAccount(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {
        throw new NotImplementedException();
    }

    private void logPriceTableProps(PriceTable priceTable, Long businessAccountOid, Long priceTableOid) {
        if (logger.isDebugEnabled()) {
            logger.debug("priceTable {}'s size {} for BA {}", () -> priceTableOid,
                    () -> priceTable.toString().length(), () -> businessAccountOid);
            logOnce(priceTableCache, String.valueOf(priceTable.hashCode()), "",
                    () -> String.format("priceTable for BA %d - (hash: %d) %s",
                            businessAccountOid, priceTable.hashCode(), priceTable), null);
        }
    }

    private void logAccountPricingDataByBA(
            Map<Long, AccountPricingData<TopologyEntityDTO>> accountPricingDataByBusinessAccountOid) {
        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (Long baOid : accountPricingDataByBusinessAccountOid.keySet()) {
                PriceTable prTab = accountPricingDataByBusinessAccountOid.get(baOid).getPriceTable();
                sb.append("BA: ").append(baOid)
                    .append(" priceTable hash: ")
                    .append(prTab.hashCode())
                    .append(" priceTable size: ")
                    .append(prTab.toString().length())
                    .append("\n");
            }
            logOnce(pricingByBACache, accountPricingDataByBusinessAccountOid.keySet().toString(), "",
                    () -> String.format("accountPricingDataByBusinessAccountOid for Biz Accounts:\n%s",
                            sb.toString()), null);
        }
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
