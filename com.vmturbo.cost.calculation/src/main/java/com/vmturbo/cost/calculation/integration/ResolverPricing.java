package com.vmturbo.cost.calculation.integration;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.PricingDataIdentifier;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;

/**
 * Abstract class for resolving pricing of business accounts to account pricing data.
 */
public abstract class ResolverPricing implements PricingResolver<TopologyEntityDTO> {
    private final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory;
    private final EntityInfoExtractor<TopologyEntityDTO> topologyEntityInfoExtractor;

    /**
     * Constructor for the Pricing Resolver.
     *
     * @param discountApplicatorFactory The discount applicator factory.
     * @param topologyEntityInfoExtractor The instance of the topology entity info extractor.
     */
    public ResolverPricing(@Nonnull DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory,
                           @Nonnull TopologyEntityInfoExtractor topologyEntityInfoExtractor) {
        this.discountApplicatorFactory = Objects.requireNonNull(discountApplicatorFactory,
                "Undefined Discount Applicator Factory.");
        this.topologyEntityInfoExtractor = Objects.requireNonNull(topologyEntityInfoExtractor,
                "Undefined Topology Entity Info Extractor.");
    }

    /**
     * Given a map of price table key oid by business account oid and priceTable by priceTableKey oid,
     * return a map of price table by business account oid.
     *
     * @param priceTableKeyOidByBusinessAccountOid The priceTableKeyOidByBusinessAccountOid map.
     * @param priceTableByPriceTableKeyOid The priceTableByPriceTableKeyOid map.
     *
     * @return The Price Table by business account mapping.
     */
    protected Map<Long, PriceTable> getPriceTableByBusinessAccountOid(Map<Long, Long> priceTableKeyOidByBusinessAccountOid,
                                                                    Map<Long, PriceTable> priceTableByPriceTableKeyOid) {
        Map<Long, PriceTable> accountPriceTableByBusinessAccountOid = new HashMap<>();
        for (Map.Entry<Long, Long> entry: priceTableKeyOidByBusinessAccountOid.entrySet()) {
            Long baOid = entry.getKey();
            Long priceTableKeyOid = entry.getValue();
            if (priceTableByPriceTableKeyOid.get(priceTableKeyOid) != null) {
                PriceTable priceTable = priceTableByPriceTableKeyOid.get(priceTableKeyOid);
                accountPriceTableByBusinessAccountOid.put(baOid, priceTable);
            }
        }
        return accountPriceTableByBusinessAccountOid;
    }

    /**
     * Create mapping entry (and put in the map passed as arguments) of BA and the pricing data for this BA
     * (including pricing table and discounts if any).
     *
     * @param cloudTopo cloud topology.
     * @param accountPricingDataMapByPricingDataIdentifier map of account pricing by pricing identifier.
     * @param discountsByAccount discounts by account map.
     * @param accountPricingDataByBusinessAccountOid pricing data by business account.
     * @param baOid BA oid.
     * @param priceTable price table.
     * @param priceTableOid price table oid.
     */
    protected void buildPricingDataByBizAccount(final CloudTopology<TopologyEntityDTO> cloudTopo,
            Map<PricingDataIdentifier, AccountPricingData<TopologyEntityDTO>> accountPricingDataMapByPricingDataIdentifier,
            final Map<Long, Discount> discountsByAccount,
            Map<Long, AccountPricingData<TopologyEntityDTO>> accountPricingDataByBusinessAccountOid,
            final Long baOid,
            final PriceTable priceTable,
            final Long priceTableOid) {
        Optional<Discount> discount = Optional.ofNullable(discountsByAccount.get(baOid));

        PricingDataIdentifier pricingDataIdentifier = new PricingDataIdentifier(discount, priceTableOid);
        if (accountPricingDataMapByPricingDataIdentifier.get(pricingDataIdentifier) != null) {
            accountPricingDataByBusinessAccountOid.put(baOid,
                    accountPricingDataMapByPricingDataIdentifier.get(pricingDataIdentifier));
        } else {
            final DiscountApplicator<TopologyEntityDTO> discountApplicator =
                    discountApplicatorFactory.accountDiscountApplicator(baOid,
                            cloudTopo, topologyEntityInfoExtractor, discount);
            final long accountPricingDataKey = IdentityGenerator.next();
            AccountPricingData<TopologyEntityDTO> accountPricingData = new AccountPricingData<>(
                    discountApplicator, priceTable, accountPricingDataKey);
            accountPricingDataMapByPricingDataIdentifier.put(pricingDataIdentifier, accountPricingData);
            accountPricingDataByBusinessAccountOid.put(baOid, accountPricingData);
        }
    }
}
