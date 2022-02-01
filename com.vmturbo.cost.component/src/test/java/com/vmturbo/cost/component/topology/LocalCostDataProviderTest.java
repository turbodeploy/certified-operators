package com.vmturbo.cost.component.topology;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.EntityReservedInstanceMappingStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.event.library.uptime.EntityUptime;
import com.vmturbo.topology.event.library.uptime.EntityUptimeStore;

public class LocalCostDataProviderTest {

    private TopologyDTO.TopologyEntityDTO cloudVm;
    private TopologyDTO.TopologyEntityDTO cloudVm2;
    private TopologyEntityCloudTopology cloudTopology;
    private TopologyDTO.TopologyInfo topoInfo;

    private final TopologyEntityInfoExtractor topologyEntityInfoExtractor = mock(TopologyEntityInfoExtractor.class);
    private final ReservedInstanceSpecStore riSpecStore = mock(ReservedInstanceSpecStore.class);
    private final PriceTableStore priceTableStore = mock(PriceTableStore.class);
    private final DiscountStore discountStore = mock(DiscountStore.class);
    private final ReservedInstanceBoughtStore riBoughtStore = mock(ReservedInstanceBoughtStore.class);
    private final BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore = mock(BusinessAccountPriceTableKeyStore.class);
    private final EntityReservedInstanceMappingStore entityRiMappingStore = mock(EntityReservedInstanceMappingStore.class);
    private final DiscountApplicatorFactory discountApplicatorFactory = mock(DiscountApplicatorFactory.class);
    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    private static final EntityUptime DEFAULT_FULL_UPTIME = EntityUptime.builder()
        .uptimePercentage(100D)
        .uptime(Duration.ofMinutes(0L))
        .totalTime(Duration.ofMinutes(0L))
        .build();

    @Before
    public void setup() {
        // This is the VM that is in the scope of Cloud Topology
        cloudVm = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD).setOid(73695157440640L)
                .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        cloudVm2 = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD).setOid(73695157440630L)
                .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                .build();

        cloudTopology = mock(TopologyEntityCloudTopology.class);
        when(cloudTopology.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE)).thenReturn(ImmutableList.of(cloudVm, cloudVm2));
        when(cloudTopology.getEntity(73695157440640L)).thenReturn(Optional.of(cloudVm));
        when(cloudTopology.getEntity(73695157440641L)).thenReturn(Optional.empty());

        topoInfo = TopologyDTO.TopologyInfo.newBuilder().setTopologyContextId(1000L)
                .setTopologyType(TopologyDTO.TopologyType.PLAN)
                .build();
    }

    @Test
    public void testGetCloudCostDataEntityUptime()
            throws CloudCostDataProvider.CloudCostDataRetrievalException {

        final EntityUptimeStore entityUptimeStore = mock(EntityUptimeStore.class);
        final EntityUptime expectedUptime =  EntityUptime.builder()
                .uptime(Duration.ofMinutes(60L))
                .totalTime(Duration.ofMinutes(120L))
                .uptimePercentage(50D)
                .build();
        when(entityUptimeStore.getDefaultUptime()).thenReturn(Optional.of(DEFAULT_FULL_UPTIME));
        when(entityUptimeStore.getUptimeByFilter(Matchers.any())).thenReturn(ImmutableMap.of(cloudVm.getOid(), expectedUptime));
        when(entityUptimeStore.getAllEntityUptime()).thenReturn(ImmutableMap.of(cloudVm.getOid(), expectedUptime));

        final LocalCostDataProvider costDataProvider = new LocalCostDataProvider(priceTableStore,
            discountStore, riBoughtStore, businessAccountPriceTableKeyStore, riSpecStore, entityRiMappingStore,
            identityProvider, discountApplicatorFactory, topologyEntityInfoExtractor, entityUptimeStore);

        CloudCostData cloudCostData = costDataProvider.getCloudCostData(topoInfo, cloudTopology, topologyEntityInfoExtractor);
        verifyUptimePercentage(cloudCostData, cloudVm.getOid(), expectedUptime.uptimePercentage());
        verifyUptimePercentage(cloudCostData, cloudVm2.getOid(), DEFAULT_FULL_UPTIME.uptimePercentage());

        // check RT plan type
        topoInfo = topoInfo.toBuilder().setTopologyType(TopologyType.REALTIME).build();
        cloudCostData = costDataProvider.getCloudCostData(topoInfo, cloudTopology, topologyEntityInfoExtractor);
        verifyUptimePercentage(cloudCostData, cloudVm.getOid(), expectedUptime.uptimePercentage());
        verifyUptimePercentage(cloudCostData, cloudVm2.getOid(), DEFAULT_FULL_UPTIME.uptimePercentage());
    }

    /**
     * Test that for allocation and consumption migrate-to-cloud plans, uptime percentage is
     * always 100%.
     *
     * @throws CloudCostDataRetrievalException never ever.
     */
    @Test
    public void testMigrateCloudPlanUptimePercentage() throws CloudCostDataRetrievalException {
        topoInfo = topoInfo.toBuilder().setTopologyType(TopologyType.PLAN)
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                .setPlanProjectType(PlanProjectType.CLOUD_MIGRATION)
                .setPlanType(PlanProjectType.CLOUD_MIGRATION.toString())
                .setPlanSubType(StringConstants.CLOUD_MIGRATION_PLAN__ALLOCATION))
            .build();

        final EntityUptimeStore entityUptimeStore = mock(EntityUptimeStore.class);
        final EntityUptime nonDefaultUptime =  EntityUptime.builder()
            .uptime(Duration.ofMinutes(60L))
            .totalTime(Duration.ofMinutes(120L))
            .uptimePercentage(50D)
            .build();
        when(entityUptimeStore.getUptimeByFilter(Matchers.any()))
            .thenReturn(ImmutableMap.of(cloudVm.getOid(), nonDefaultUptime));
        when(entityUptimeStore.getAllEntityUptime())
            .thenReturn(ImmutableMap.of(cloudVm.getOid(), nonDefaultUptime));
        when(entityUptimeStore.getDefaultUptime()).thenReturn(Optional.of(nonDefaultUptime));

        LocalCostDataProvider costDataProvider = new LocalCostDataProvider(priceTableStore,
            discountStore, riBoughtStore, businessAccountPriceTableKeyStore, riSpecStore,
            entityRiMappingStore, identityProvider, discountApplicatorFactory,
            topologyEntityInfoExtractor, entityUptimeStore);

        final CloudCostData resultAllocation =
            costDataProvider.getCloudCostData(topoInfo, cloudTopology, topologyEntityInfoExtractor);
        verifyUptimePercentage(resultAllocation, cloudVm.getOid(), DEFAULT_FULL_UPTIME.uptimePercentage());

        topoInfo = topoInfo.toBuilder().setTopologyType(TopologyType.PLAN)
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                .setPlanProjectType(PlanProjectType.CLOUD_MIGRATION)
                .setPlanType(PlanProjectType.CLOUD_MIGRATION.toString())
                .setPlanSubType(StringConstants.CLOUD_MIGRATION_PLAN__CONSUMPTION))
            .build();
        final CloudCostData resultConsumption =
            costDataProvider.getCloudCostData(topoInfo, cloudTopology, topologyEntityInfoExtractor);
        verifyUptimePercentage(resultConsumption, cloudVm.getOid(), DEFAULT_FULL_UPTIME.uptimePercentage());
    }

    private void verifyUptimePercentage(CloudCostData data, Long oid, Double expectedUptime) {
        Double actualEntityUptime = data.getEntityUptimePercentage(oid);
        assertEquals(actualEntityUptime, expectedUptime, 0.0001D);
    }
}
