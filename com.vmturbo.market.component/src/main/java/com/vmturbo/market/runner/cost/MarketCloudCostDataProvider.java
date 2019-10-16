package com.vmturbo.market.runner.cost;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterResponse;
import com.vmturbo.common.protobuf.cost.BuyReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.BuyReservedInstanceServiceGrpc.BuyReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc.ReservedInstanceSpecServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;

/**
 * An implementation of {@link CloudCostDataProvider} that gets the relevant
 * {@link CloudCostData} via RPCs in the cost component.
 */
public class MarketCloudCostDataProvider implements CloudCostDataProvider {

    private final PricingServiceBlockingStub pricingServiceClient;

    private final CostServiceBlockingStub costServiceClient;

    private final ReservedInstanceBoughtServiceBlockingStub riBoughtServiceClient;

    private final ReservedInstanceSpecServiceBlockingStub riSpecServiceClient;

    private final BuyReservedInstanceServiceBlockingStub buyRIServiceClient;

    public MarketCloudCostDataProvider(@Nonnull final Channel costChannel) {
        this.pricingServiceClient = Objects.requireNonNull(PricingServiceGrpc.newBlockingStub(costChannel));
        this.costServiceClient = Objects.requireNonNull(CostServiceGrpc.newBlockingStub(costChannel));
        this.riBoughtServiceClient = Objects.requireNonNull(ReservedInstanceBoughtServiceGrpc.newBlockingStub(costChannel));
        this.riSpecServiceClient = Objects.requireNonNull(ReservedInstanceSpecServiceGrpc.newBlockingStub(costChannel));
        this.buyRIServiceClient = Objects.requireNonNull(BuyReservedInstanceServiceGrpc.newBlockingStub(costChannel));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public CloudCostData getCloudCostData(@Nonnull TopologyInfo topoInfo)
                    throws CloudCostDataRetrievalException {
        try {
            // Get the price table.
            final PriceTable priceTable = pricingServiceClient.getPriceTable(
                    GetPriceTableRequest.getDefaultInstance()).getGlobalPriceTable();

            // Get the discounts.
            final Map<Long, Discount> discountsByAccount = new HashMap<>();
            costServiceClient.getDiscounts(GetDiscountRequest.getDefaultInstance())
                .forEachRemaining(discount -> discountsByAccount.put(discount.getAssociatedAccountId(), discount));

            // Get the existing RI bought.
            final GetReservedInstanceBoughtByFilterResponse riBoughtResponse =
                riBoughtServiceClient.getReservedInstanceBoughtByFilter(
                    GetReservedInstanceBoughtByFilterRequest.newBuilder()
                    .addAllScopeSeedOids(topoInfo.getScopeSeedOidsList())
                    .setScopeEntityType(topoInfo.getScopeEntityType())
                    .build());
            final Map<Long, ReservedInstanceBought> riBoughtById =
                    new HashMap<>(riBoughtResponse.getReservedInstanceBoughtsCount());

            // Get the new RI bought.
            final GetBuyReservedInstancesByFilterResponse buyRIBoughtResponse =
                    buyRIServiceClient.getBuyReservedInstancesByFilter(GetBuyReservedInstancesByFilterRequest
                            .newBuilder().setTopologyContextId(topoInfo.getTopologyContextId())
                            .build());
            final Map<Long, ReservedInstanceBought> buyRIBoughtById = new HashMap<>();
            // While processing the RI bought, collect the specs we need to retrieve.
            // There are A LOT of RI specs, and it would be very wasteful to retrieve all of them.
            // Retrieve only the ones that are referenced to by existing RI purchases.
            // Use a set to remove duplicates.
            final Set<Long> riSpecIdsToRetrieve = new HashSet<>();
            riBoughtResponse.getReservedInstanceBoughtsList().forEach(riBought -> {
                riBoughtById.put(riBought.getId(), riBought);
                riSpecIdsToRetrieve.add(
                    riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec());
            });
            // also get the riSpec for buy RI
            buyRIBoughtResponse.getReservedInstanceBoughtsList().forEach(riBought -> {
                buyRIBoughtById.put(riBought.getId(), riBought);
                riSpecIdsToRetrieve.add(
                    riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec());
            });

            // Get the RI specs.
            final GetReservedInstanceSpecByIdsResponse riSpecResponse =
                riSpecServiceClient.getReservedInstanceSpecByIds(
                    GetReservedInstanceSpecByIdsRequest.newBuilder()
                        .addAllReservedInstanceSpecIds(riSpecIdsToRetrieve)
                        .build());
            final Map<Long, ReservedInstanceSpec> riSpecsById =
                riSpecResponse.getReservedInstanceSpecList().stream()
                    .collect(Collectors.toMap(ReservedInstanceSpec::getId, Function.identity()));

            // Get the entity RI coverage.
            final GetEntityReservedInstanceCoverageResponse coverageResponse =
                riBoughtServiceClient.getEntityReservedInstanceCoverage(
                    GetEntityReservedInstanceCoverageRequest.getDefaultInstance());

            return new CloudCostData(priceTable,
                    discountsByAccount,
                    coverageResponse.getCoverageByEntityIdMap(),
                    riBoughtById,
                    riSpecsById,
                    buyRIBoughtById);
        } catch (StatusRuntimeException e) {
            throw new CloudCostDataRetrievalException(e);
        }
    }
}