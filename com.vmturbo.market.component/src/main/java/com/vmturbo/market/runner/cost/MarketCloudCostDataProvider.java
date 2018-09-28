package com.vmturbo.market.runner.cost;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.GetDiscountRequest;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;

/**
 * An implementation of {@link CloudCostDataProvider} that gets the relevant
 * {@link CloudCostData} via RPCs in the cost component.
 */
public class MarketCloudCostDataProvider implements CloudCostDataProvider {

    private final PricingServiceBlockingStub pricingServiceClient;

    private final CostServiceBlockingStub costServiceClient;

    public MarketCloudCostDataProvider(@Nonnull final Channel costChannel) {
        this.pricingServiceClient = Objects.requireNonNull(PricingServiceGrpc.newBlockingStub(costChannel));
        this.costServiceClient = Objects.requireNonNull(CostServiceGrpc.newBlockingStub(costChannel));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public CloudCostData getCloudCostData() throws CloudCostDataRetrievalException {
        try {
            final PriceTable priceTable = pricingServiceClient.getPriceTable(
                    GetPriceTableRequest.getDefaultInstance()).getGlobalPriceTable();
            final Map<Long, Discount> discountsByAccount = new HashMap<>();
            costServiceClient.getDiscounts(GetDiscountRequest.getDefaultInstance())
                .forEachRemaining(discount -> discountsByAccount.put(discount.getAssociatedAccountId(), discount));
            // TODO (roman, Sept 25 2018): Add RI data as well.
            return new CloudCostData(priceTable, discountsByAccount,
                    Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        } catch (StatusRuntimeException e) {
            throw new CloudCostDataRetrievalException(e);
        }
    }
}