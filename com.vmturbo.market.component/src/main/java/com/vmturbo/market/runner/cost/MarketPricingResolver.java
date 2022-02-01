package com.vmturbo.market.runner.cost;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTablesResponse;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableChunk;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableChunk.OnDemandLicensePriceEntrySegment;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableChunk.OnDemandPriceTableByRegionSegment;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableChunk.ReservedLicenseSegment;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableChunk.SpotInstancePriceTableByRegionSegment;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.ResolverPricing;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.PricingDataIdentifier;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.PricingDTO.LicenseOverrides;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry;

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
        final Map<Long, PriceTable> priceTableByPriceTableKeyOid = getPriceTablesSegments(priceTableKeyOidByBusinessAccountOid);

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


    @Nonnull
    private Map<Long, PriceTable> getPriceTablesSegments(@Nonnull final Map<Long, Long> priceTableKeyOidByBusinessAccountOid) {
        final Map<Long, PriceTable.Builder> priceTableBuilderByOidMap = new HashMap<>();
        final Map<Long, Long> priceTableSerializedSizeByOid = new HashMap<>();
        Iterator<GetPriceTablesResponse> priceTableSegmentIter = pricingServiceClient.getPriceTables(
                GetPriceTablesRequest.newBuilder().addAllOid(priceTableKeyOidByBusinessAccountOid.values())
                        .build());
        while (priceTableSegmentIter.hasNext()) {
            final GetPriceTablesResponse response = priceTableSegmentIter.next();
            final long serializedSize = response.getPriceTableSerializedSize();
            final List<PriceTableChunk> priceTableChunks = response.getPriceTableChunkList();
            for (PriceTableChunk priceTableChunk : priceTableChunks) {
                final long priceTableOid = priceTableChunk.getPriceTableOid();
                priceTableSerializedSizeByOid.putIfAbsent(priceTableOid, serializedSize);
                try {
                    switch (priceTableChunk.getPriceTableSegmentCase()) {
                        case ONDEMAND_PRICE_TABLE_BY_REGION:
                            captureOndemandPriceTable(priceTableBuilderByOidMap, priceTableOid,
                                    priceTableChunk.getOndemandPriceTableByRegion());
                            break;
                        case SPOT_INSTANCE_PRICE_TABLE_BY_REGION:
                            captureSpotInstancePrice(priceTableBuilderByOidMap, priceTableOid,
                                    priceTableChunk.getSpotInstancePriceTableByRegion());
                            break;
                        case RESERVED_LICENSE_SEGMENT:
                            captureReservedLicensePrice(priceTableBuilderByOidMap, priceTableOid,
                                    priceTableChunk.getReservedLicenseSegment());
                            break;
                        case ONDEMAND_LICENSE_PRICE_ENTRY_SEGMENT:
                            captureOnDemandLicensePriceEntry(priceTableBuilderByOidMap, priceTableOid,
                                    priceTableChunk.getOndemandLicensePriceEntrySegment());
                            break;
                        case PRICETABLESEGMENT_NOT_SET:
                        default:
                            logger.error("Price table segment not found for priceTable OID {}",
                                    priceTableChunk.getPriceTableOid());
                            break;
                    }
                } catch (Exception e) {
                    logger.error("Error occurred while capturing priceTableSegment for priceTable OID {}.",
                            priceTableChunk.getPriceTableOid(), e);
                }
            }
        }
        final Map<Long, PriceTable> resultMap = priceTableBuilderByOidMap.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().build()));
        final boolean transferSuccessful = priceTableSerializedSizeByOid.entrySet().stream().noneMatch((entry) -> {
            final PriceTable priceTable = resultMap.get(entry.getKey());
            return (priceTable == null || priceTable.getSerializedSize() != entry.getValue());
        });
        if (!transferSuccessful) {
            logger.error("Price table serialized sizes did not match. This can lead to inconsistent prices");
        }
        return resultMap;
    }

    private void captureOnDemandLicensePriceEntry(@Nonnull final Map<Long, PriceTable.Builder> result,
                                                  final long priceTableOid,
                                                  @Nonnull final OnDemandLicensePriceEntrySegment ondemandLicensePriceEntrySegment) {
        final List<LicensePriceEntry> licensePriceEntries = ondemandLicensePriceEntrySegment.getLicensePriceEntryList();
        final Map<Long, LicenseOverrides> onDemandLicenseOverridesMap = ondemandLicensePriceEntrySegment.getOnDemandLicenseOverridesMap();
        result.compute(priceTableOid, (currentOid, currentPriceTable) -> {
            if (currentPriceTable == null) {
                currentPriceTable = PriceTable.newBuilder();
            }
            currentPriceTable.addAllOnDemandLicensePrices(licensePriceEntries);
            currentPriceTable.putAllOnDemandLicenseOverrides(onDemandLicenseOverridesMap);
            return currentPriceTable;
        });
    }

    private void captureReservedLicensePrice(@Nonnull final Map<Long, PriceTable.Builder> priceTableByRegionId,
                                             final long priceTableOid,
                                             @Nonnull final ReservedLicenseSegment reservedLicenseSegment) {
        final List<LicensePriceEntry> reservedLicensePriceEntryList = reservedLicenseSegment.getReservedLicensePriceEntryList();
        final Map<Long, LicenseOverrides> reservedLicenseOverridesMap = reservedLicenseSegment.getReservedLicenseOverridesMap();
        priceTableByRegionId.compute(priceTableOid, (currentOid, currentPriceTable) -> {
            if (currentPriceTable == null) {
                currentPriceTable = PriceTable.newBuilder();
            }
            currentPriceTable.addAllReservedLicensePrices(reservedLicensePriceEntryList);
            currentPriceTable.putAllReservedLicenseOverrides(reservedLicenseOverridesMap);
            return currentPriceTable;
        });

    }

    private void captureSpotInstancePrice(@Nonnull final Map<Long, PriceTable.Builder> priceTableByRegionId,
                                          final long priceTableOid,
                                          @Nonnull final SpotInstancePriceTableByRegionSegment spotInstancePriceTableByRegion) {
        final long regionId = spotInstancePriceTableByRegion.getRegionId();
        final SpotInstancePriceTable spotInstancePriceTable = spotInstancePriceTableByRegion.getSpotInstancePriceTable();
        priceTableByRegionId.compute(priceTableOid, (currentOid, currentPriceTable) -> {
            if (currentPriceTable == null) {
                currentPriceTable = PriceTable.newBuilder();
            }
            currentPriceTable.putSpotPriceByZoneOrRegionId(regionId, spotInstancePriceTable);
            return currentPriceTable;
        });
    }

    private void captureOndemandPriceTable(@Nonnull final Map<Long, PriceTable.Builder> priceTableByRegionId,
                                           final long priceTableOid,
                                           @Nonnull final OnDemandPriceTableByRegionSegment ondemandPriceTableByRegion) {
        final long regionId = ondemandPriceTableByRegion.getRegionId();
        final OnDemandPriceTable onDemandPriceTable = ondemandPriceTableByRegion.getOnDemandPriceTable();
        priceTableByRegionId.compute(priceTableOid, (currentOid, currentPriceTable) -> {
            if (currentPriceTable == null) {
                currentPriceTable = PriceTable.newBuilder();
            }
            currentPriceTable.putOnDemandPriceByRegionId(regionId, onDemandPriceTable);
            return currentPriceTable;
        });
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
