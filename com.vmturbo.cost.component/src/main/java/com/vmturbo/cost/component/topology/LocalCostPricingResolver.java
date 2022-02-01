package com.vmturbo.cost.component.topology;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.ResolverPricing;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.PricingDataIdentifier;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbException;

/**
 * A class for resolving the mapping of business accounts to account pricing data.
 */
public class LocalCostPricingResolver extends ResolverPricing {
    private static final Logger logger = LogManager.getLogger();

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
        logger.debug("priceTableKeyByBusinessAccount = {}", priceTableKeyOidByBusinessAccountOid);

        Collection<Long> prTabKeyOids = priceTableKeyOidByBusinessAccountOid.values();
        final Map<Long, PriceTable> priceTableByPriceTableKeyOid = priceTableStore.getPriceTables(
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

            buildPricingDataByBizAccount(cloudTopo, accountPricingDataMapByPricingDataIdentifier, discountsByAccount,
                    accountPricingDataByBusinessAccountOid, baOid, priceTable, priceTableKeyOid);
        }
        return accountPricingDataByBusinessAccountOid;
    }

    @Override
    public Map<Long, ReservedInstancePriceTable> getRIPriceTablesByAccount(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {

        Set<Long> accountOids = cloudTopology.getAllEntitiesOfType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .stream()
                .map(TopologyEntityDTO::getOid)
                .collect(Collectors.toSet());

        Map<Long, Long> priceTableKeyOidByBusinessAccountOid = ImmutableMap.copyOf(
                businessAccountPriceTableKeyStore.fetchPriceTableKeyOidsByBusinessAccount(accountOids));
        Map<Long, ReservedInstancePriceTable> riPriceTableByPriceTableKeyOid =
                priceTableStore.getRiPriceTables(priceTableKeyOidByBusinessAccountOid.values());

        // The RI price table indexed by the business account oid.
        final ImmutableMap.Builder<Long, ReservedInstancePriceTable> priceTableMap = ImmutableMap.builder();
        for (Map.Entry<Long, Long> entry: priceTableKeyOidByBusinessAccountOid.entrySet()) {

            final Long accountOid = entry.getKey();
            final Long priceTableKeyOid = entry.getValue();

            if (riPriceTableByPriceTableKeyOid.containsKey(priceTableKeyOid)) {
                ReservedInstancePriceTable priceTable = riPriceTableByPriceTableKeyOid.get(priceTableKeyOid);
                priceTableMap.put(accountOid, priceTable);
            } else {
                logger.error("Unable to find RI price table for account (Account OID={} Price Table Key={})",
                        accountOid, priceTableKeyOid);
            }
        }

        return priceTableMap.build();
    }
}
