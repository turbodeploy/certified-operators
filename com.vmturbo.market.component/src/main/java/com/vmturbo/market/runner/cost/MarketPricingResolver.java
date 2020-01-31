package com.vmturbo.market.runner.cost;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.GetDiscountRequest;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.Pricing.GetAccountPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTablesRequest;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
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

    private final PricingServiceBlockingStub pricingServiceClient;

    private final CostServiceBlockingStub costServiceClient;

    private final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory;

    private final EntityInfoExtractor<TopologyEntityDTO> topologyEntityInfoExtractor;

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
        this.pricingServiceClient = Objects.requireNonNull(pricingServiceClient);
        this.costServiceClient = Objects.requireNonNull(costServiceClient);
        this.discountApplicatorFactory = Objects.requireNonNull(discountApplicatorFactory);
        this.topologyEntityInfoExtractor = topologyEntityInfoExtractor;
    }

    @Override
    public Map<Long, AccountPricingData<TopologyEntityDTO>> getAccountPricingDataByBusinessAccount(
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopo) {
        // Get the discounts.
        final Map<Long, Discount> discountsByAccount = new HashMap<>();
        costServiceClient.getDiscounts(GetDiscountRequest.getDefaultInstance())
                .forEachRemaining(discount -> discountsByAccount.put(discount.getAssociatedAccountId(), discount));

        Map<Long, AccountPricingData<TopologyEntityDTO>> accountPricingDatabyBusinessAccountOid
                = new HashMap<>();
        // Get the set of business accounts in the topology.
        Set<Long> baOids = cloudTopo.getAllEntitiesOfType(EntityType.BUSINESS_ACCOUNT_VALUE).stream()
                .map(TopologyEntityDTO::getOid).collect(Collectors.toSet());

        // Get a mapping of business account oid -> price table key oid.
        final Map<Long, Long> priceTableKeyOidByBusinessAccountOid = pricingServiceClient.getAccountPriceTable(GetAccountPriceTableRequest.newBuilder()
                .addAllBusinessAccountOid(baOids).build()).getBusinessAccountPriceTableKeyMap();

        //Get a mapping of price table key oid to price table.
        final Map<Long, PriceTable> priceTableByPriceTableKeyOid = pricingServiceClient.getPriceTables(GetPriceTablesRequest.newBuilder()
                .addAllOid(priceTableKeyOidByBusinessAccountOid.values()).build()).getPriceTablesByOidMap();

        Map<Long, PriceTable> priceTableByBusinessAccountOid = getPriceTableByBusinessAccountOid(priceTableKeyOidByBusinessAccountOid,
                priceTableByPriceTableKeyOid);
        for (Map.Entry<Long, PriceTable> entry : priceTableByBusinessAccountOid.entrySet()) {
            Long businessAccountOid = entry.getKey();
            PriceTable priceTable = entry.getValue();
            Optional<Discount> discount = Optional.ofNullable(discountsByAccount.get(businessAccountOid));
            PricingDataIdentifier pricingDataIdentifier = new PricingDataIdentifier(discount, priceTable);
            if (accountPricingDataMapByPricingDataIdentifier.get(pricingDataIdentifier) != null) {
                accountPricingDatabyBusinessAccountOid.put(businessAccountOid, accountPricingDataMapByPricingDataIdentifier
                        .get(pricingDataIdentifier));
            } else {
                final DiscountApplicator<TopologyEntityDTO> discountApplicator =
                        discountApplicatorFactory.accountDiscountApplicator(businessAccountOid,
                        cloudTopo, topologyEntityInfoExtractor, discount);
                final AtomicLong accountPricingDataKey = new AtomicLong(IdentityGenerator.next());
                PricingDataIdentifier newPricingDataIdentifier = new PricingDataIdentifier(discount, priceTable);
                AccountPricingData<TopologyEntityDTO> accountPricingData = new AccountPricingData<>(
                        discountApplicator, priceTable, accountPricingDataKey.longValue());
                accountPricingDataMapByPricingDataIdentifier.put(newPricingDataIdentifier, accountPricingData);
                accountPricingDatabyBusinessAccountOid.put(businessAccountOid, accountPricingData);
            }
        }
        return accountPricingDatabyBusinessAccountOid;
    }
}
