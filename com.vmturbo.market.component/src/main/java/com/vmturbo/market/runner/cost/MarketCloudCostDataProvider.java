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

import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterResponse;
import com.vmturbo.common.protobuf.cost.BuyReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.BuyReservedInstanceServiceGrpc.BuyReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByTopologyRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByTopologyResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc.ReservedInstanceSpecServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;

/**
 * An implementation of {@link CloudCostDataProvider} that gets the relevant
 * {@link CloudCostData} via RPCs in the cost component.
 */
public class MarketCloudCostDataProvider implements CloudCostDataProvider {

    private final ReservedInstanceBoughtServiceBlockingStub riBoughtServiceClient;

    private final ReservedInstanceSpecServiceBlockingStub riSpecServiceClient;

    private final BuyReservedInstanceServiceBlockingStub buyRIServiceClient;

    private final MarketPricingResolver marketPricingResolver;

    private final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationServiceClient;

    /**
     * Constructor for the market cloud cost data provider.
     *
     * @param costChannel The cost channel required for constructing the clients.
     * @param discountApplicatorFactory The discount applicator factory instance.
     * @param topologyEntityInfoExtractor The topology entity info extractor.
     */
    public MarketCloudCostDataProvider(@Nonnull final Channel costChannel,
                                       @Nonnull DiscountApplicatorFactory<TopologyEntityDTO>
                                               discountApplicatorFactory,
                                       @Nonnull TopologyEntityInfoExtractor topologyEntityInfoExtractor) {
        this.riBoughtServiceClient = Objects.requireNonNull(ReservedInstanceBoughtServiceGrpc.newBlockingStub(costChannel));
        this.riSpecServiceClient = Objects.requireNonNull(ReservedInstanceSpecServiceGrpc.newBlockingStub(costChannel));
        this.buyRIServiceClient = Objects.requireNonNull(BuyReservedInstanceServiceGrpc.newBlockingStub(costChannel));
        this.marketPricingResolver = new MarketPricingResolver(
                PricingServiceGrpc.newBlockingStub(costChannel),
                Objects.requireNonNull(CostServiceGrpc.newBlockingStub(costChannel)),
                discountApplicatorFactory, topologyEntityInfoExtractor);
        this.riUtilizationServiceClient =
                Objects.requireNonNull(ReservedInstanceUtilizationCoverageServiceGrpc
                        .newBlockingStub(costChannel));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public CloudCostData<TopologyEntityDTO> getCloudCostData(@Nonnull TopologyInfo topoInfo,
    @Nonnull CloudTopology<TopologyEntityDTO> cloudTopo,
                                          @Nonnull final TopologyEntityInfoExtractor topologyEntityInfoExtractor)
                    throws CloudCostDataRetrievalException {
        try {

            //Get a mapping of business account oid to Account Pricing Data.
            final Map<Long, AccountPricingData<TopologyEntityDTO>>
                    accountPricingDataByBusinessAccountOid = marketPricingResolver
                    .getAccountPricingDataByBusinessAccount(cloudTopo);

            // Get the existing RI bought.
            final GetReservedInstanceBoughtByTopologyResponse riBoughtResponse =
                riBoughtServiceClient.getReservedInstanceBoughtByTopology(
                        GetReservedInstanceBoughtByTopologyRequest.newBuilder()
                                .setTopologyType(topoInfo.getTopologyType())
                                .setTopologyContextId(topoInfo.getTopologyContextId())
                                .setScopeEntityType(topoInfo.getScopeEntityType())
                                .addAllScopeSeedOids(topoInfo.getScopeSeedOidsList())
                                .build());
            final Map<Long, ReservedInstanceBought> riBoughtById =
                    new HashMap<>(riBoughtResponse.getReservedInstanceBoughtCount());

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
            riBoughtResponse.getReservedInstanceBoughtList().forEach(riBought -> {
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
                            riUtilizationServiceClient.getEntityReservedInstanceCoverage(
                    GetEntityReservedInstanceCoverageRequest.getDefaultInstance());

            return new CloudCostData<>(coverageResponse.getCoverageByEntityIdMap(),
                    riBoughtById,
                    riSpecsById,
                    buyRIBoughtById, accountPricingDataByBusinessAccountOid);
        } catch (StatusRuntimeException e) {
            throw new CloudCostDataRetrievalException(e);
        }
    }
}
