package com.vmturbo.cost.component.topology;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.EntityReservedInstanceMappingStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.event.library.uptime.EntityUptime;
import com.vmturbo.topology.event.library.uptime.EntityUptimeStore;

/**
 * A {@link CloudCostDataProvider} that gets the data locally from within the cost component.
 */
public class LocalCostDataProvider implements CloudCostDataProvider {

    private final PriceTableStore priceTableStore;

    private final DiscountStore discountStore;

    private final ReservedInstanceBoughtStore riBoughtStore;

    private final BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore;

    private final LocalCostPricingResolver localCostPricingResolver;

    private final ReservedInstanceSpecStore riSpecStore;

    private final EntityReservedInstanceMappingStore entityRiMappingStore;

    private final EntityUptimeStore entityUptimeStore;

    public LocalCostDataProvider(@Nonnull final PriceTableStore priceTableStore,
                 @Nonnull final DiscountStore discountStore,
                 @Nonnull final ReservedInstanceBoughtStore riBoughtStore,
                final BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore,
                 @Nonnull final ReservedInstanceSpecStore riSpecStore,
                 @Nonnull final EntityReservedInstanceMappingStore entityRiMappingStore,
                                 IdentityProvider identityProvider,
                                 @Nonnull DiscountApplicatorFactory discountApplicatorFactory,
                                 @Nonnull TopologyEntityInfoExtractor topologyEntityInfoExtractor,
                                 @Nonnull EntityUptimeStore entityUptimeStore) {
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
        this.discountStore = Objects.requireNonNull(discountStore);
        this.riBoughtStore = Objects.requireNonNull(riBoughtStore);
        this.businessAccountPriceTableKeyStore = Objects.requireNonNull(businessAccountPriceTableKeyStore);
        this.riSpecStore = Objects.requireNonNull(riSpecStore);
        this.entityRiMappingStore = Objects.requireNonNull(entityRiMappingStore);
        this.localCostPricingResolver = new LocalCostPricingResolver(priceTableStore,
                businessAccountPriceTableKeyStore, identityProvider, discountStore,
                discountApplicatorFactory, topologyEntityInfoExtractor);
        this.entityUptimeStore = entityUptimeStore;
    }

    @Nonnull
    @Override
    public CloudCostData getCloudCostData(TopologyInfo topoInfo, @Nonnull CloudTopology<TopologyEntityDTO> cloudTopo,
                TopologyEntityInfoExtractor topologyEntityInfoExtractor) throws CloudCostDataRetrievalException {
        final Map<Long, AccountPricingData<TopologyEntityDTO>> accountPricingIdByBusinessAccountOid
                = localCostPricingResolver.getAccountPricingDataByBusinessAccount(cloudTopo);
        final Map<Long, ReservedInstanceBought> riBoughtById =
            riBoughtStore.getReservedInstanceBoughtByFilter(ReservedInstanceBoughtFilter
                        .newBuilder().build()).stream()
                .collect(Collectors.toMap(ReservedInstanceBought::getId, Function.identity()));
        final Map<Long, ReservedInstanceSpec> riSpecById = riSpecStore.getAllReservedInstanceSpec().stream()
                .collect(Collectors.toMap(ReservedInstanceSpec::getId, Function.identity()));
        final Map<Long, Double> entityUptimePercentageByOid = new HashMap<>();
        Map<Long, EntityUptime> entityUptimeByOid = entityUptimeStore.getAllEntityUptime();
        if (TopologyDTO.TopologyType.PLAN == topoInfo.getTopologyType()) {
            final Set<Long> vmOidsSet = cloudTopo.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE).stream()
                    .map(TopologyEntityDTO::getOid).collect(Collectors.toSet());
            entityUptimeByOid = entityUptimeByOid.entrySet().stream()
                    .filter(entry -> vmOidsSet.contains(entry.getKey()))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        }
        entityUptimeByOid.entrySet().forEach(entry ->
                        entityUptimePercentageByOid.put(entry.getKey(), entry.getValue().uptimePercentage()));
        Optional<EntityUptime> defaultUptime = entityUptimeStore.getDefaultUptime();
        final Optional<Double> defaultUptimePercentage = defaultUptime.map(EntityUptime::uptimePercentage);
        return new CloudCostData<>(entityRiMappingStore.getEntityRiCoverage(), entityRiMappingStore.getEntityRiCoverage(),
                riBoughtById, riSpecById, Collections.emptyMap(),
                accountPricingIdByBusinessAccountOid, entityUptimePercentageByOid, defaultUptimePercentage);
    }
}

