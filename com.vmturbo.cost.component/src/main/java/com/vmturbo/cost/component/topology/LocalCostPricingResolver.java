package com.vmturbo.cost.component.topology;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
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

    private final BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore;

    private final PriceTableStore priceTableStore;

    private final IdentityProvider identityProvider;

    private final DiscountStore discountStore;

    private final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory;

    private final EntityInfoExtractor<TopologyEntityDTO> topologyEntityInfoExtractor;

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
        this.businessAccountPriceTableKeyStore = Objects.requireNonNull(businessAccountPriceTableKeyStore);
        this.priceTableStore = priceTableStore;
        this.identityProvider = identityProvider;
        this.discountStore = discountStore;
        this.discountApplicatorFactory = discountApplicatorFactory;
        this.topologyEntityInfoExtractor = topologyEntityInfoExtractor;
    }

    @Override
    public Map<Long, AccountPricingData<TopologyEntityDTO>> getAccountPricingDataByBusinessAccount(
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopo)
            throws CloudCostDataRetrievalException {
        Map<Long, AccountPricingData<TopologyEntityDTO>> accountPricingDataByBusinessAccountOid =
                new HashMap<>();
        Set<Long> baOids = cloudTopo.getAllEntitiesOfType(EntityType.BUSINESS_ACCOUNT_VALUE).stream()
                .map(TopologyEntityDTO::getOid).collect(Collectors.toSet());

        final Map<Long, Long> priceTableKeyOidByBusinessAccountOid = businessAccountPriceTableKeyStore
                .fetchPriceTableKeyOidsByBusinessAccount(baOids);

        final Map<Long, PriceTable> priceTableByPriceTableKeyOid = priceTableStore
                .getPriceTables(priceTableKeyOidByBusinessAccountOid.values());

        final Map<Long, Discount> discountsByAccountId;
        try {
            discountsByAccountId = discountStore.getAllDiscount().stream()
                    .collect(Collectors.toMap(Discount::getAssociatedAccountId, Function.identity()));
        } catch (DbException e) {
            throw new CloudCostDataRetrievalException(e);
        }

        Map<Long, PriceTable> priceTableByBusinessAccountOid = getPriceTableByBusinessAccountOid(priceTableKeyOidByBusinessAccountOid,
                priceTableByPriceTableKeyOid);
        for (Map.Entry<Long, PriceTable> entry : priceTableByBusinessAccountOid.entrySet()) {
            Long businessAccountOid = entry.getKey();
            PriceTable priceTable = entry.getValue();
            Optional<Discount> discount = Optional.ofNullable(discountsByAccountId.get(businessAccountOid));
            PricingDataIdentifier pricingDataIdentifier = new PricingDataIdentifier(discount, priceTable);
            if (accountPricingDataMapByPricingDataIdentifier.get(pricingDataIdentifier) != null) {
                accountPricingDataByBusinessAccountOid.put(businessAccountOid,
                        accountPricingDataMapByPricingDataIdentifier
                        .get(pricingDataIdentifier));
            } else {
                final AtomicLong accountProviderOid = new AtomicLong(identityProvider.next());
                final DiscountApplicator<TopologyEntityDTO> discountApplicator =
                        discountApplicatorFactory.accountDiscountApplicator(businessAccountOid,
                        cloudTopo, topologyEntityInfoExtractor, discount);
                AccountPricingData<TopologyEntityDTO> accountPricingData =
                        new AccountPricingData<>(discountApplicator, priceTable,
                                accountProviderOid.longValue());
                PricingDataIdentifier newPricingDataIdentifier = new PricingDataIdentifier(discount, priceTable);
                accountPricingDataMapByPricingDataIdentifier.putIfAbsent(newPricingDataIdentifier,
                        accountPricingData);
                accountPricingDataByBusinessAccountOid.put(businessAccountOid, accountPricingData);
            }
        }
        return accountPricingDataByBusinessAccountOid;
    }
}
