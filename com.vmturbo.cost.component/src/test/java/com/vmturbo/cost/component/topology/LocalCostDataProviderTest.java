package com.vmturbo.cost.component.topology;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
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
    TopologyEntityInfoExtractor topologyEntityInfoExtractor;
    SupplyChainServiceBlockingStub supplyChainServiceBlockingStub;

    @Before
    public void setup() throws IOException {
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

        topologyEntityInfoExtractor = mock(TopologyEntityInfoExtractor.class);

        SupplyChainServiceMole supplyChainService =
                new SupplyChainServiceMole();
        GrpcTestServer grpcTestServer = GrpcTestServer.newServer(supplyChainService);
        grpcTestServer.start();
        supplyChainServiceBlockingStub =
                SupplyChainServiceGrpc.newBlockingStub(grpcTestServer.getChannel());

    }

    @Test
    public void testGetCloudCostDataEntityUptime()
            throws CloudCostDataProvider.CloudCostDataRetrievalException, IOException {

        PriceTableStore priceTableStore = mock(PriceTableStore.class);
        DiscountStore discountStore = mock(DiscountStore.class);
        ReservedInstanceBoughtStore riBoughtStore = mock(ReservedInstanceBoughtStore.class);
        BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore = mock(BusinessAccountPriceTableKeyStore.class);
        ReservedInstanceSpecStore riSpecStore = mock(ReservedInstanceSpecStore.class);
        EntityReservedInstanceMappingStore entityRiMappingStore = mock(EntityReservedInstanceMappingStore.class);


        DiscountApplicatorFactory discountApplicatorFactory = mock(DiscountApplicatorFactory.class);
        EntityUptimeStore entityUptimeStore = mock(EntityUptimeStore.class);

        EntityUptime expectedUptime =  com.vmturbo.topology.event.library.uptime.EntityUptime.builder()
                .uptime(Duration.ofMinutes(60L))
                .totalTime(Duration.ofMinutes(120L))
                .uptimePercentage(50D)
                .build();
        EntityUptime defaultUptime = com.vmturbo.topology.event.library.uptime.EntityUptime.builder()
                .uptimePercentage(100D)
                .uptime(Duration.ofMinutes(0L))
                .totalTime(Duration.ofMinutes(0L))
                .build();
        when(entityUptimeStore.getDefaultUptime()).thenReturn(Optional.of(defaultUptime));
        when(entityUptimeStore.getUptimeByFilter(Matchers.any())).thenReturn(ImmutableMap.of(cloudVm.getOid(), expectedUptime));
        when(entityUptimeStore.getAllEntityUptime()).thenReturn(ImmutableMap.of(cloudVm.getOid(), expectedUptime));

        IdentityProvider identityProvider = mock(IdentityProvider.class);

        LocalCostDataProvider costDataProvider = new LocalCostDataProvider(priceTableStore,
         discountStore, riBoughtStore, businessAccountPriceTableKeyStore, riSpecStore, entityRiMappingStore,
                identityProvider, discountApplicatorFactory, topologyEntityInfoExtractor, entityUptimeStore);

        CloudCostData cloudCostData = costDataProvider.getCloudCostData(topoInfo, cloudTopology, topologyEntityInfoExtractor);
        verifyUptimePercentage(cloudCostData, cloudVm.getOid(), expectedUptime.uptimePercentage());
        verifyUptimePercentage(cloudCostData, cloudVm2.getOid(), defaultUptime.uptimePercentage());

        // check RT plan type
        topoInfo = topoInfo.toBuilder().setTopologyType(TopologyType.REALTIME).build();
        cloudCostData = costDataProvider.getCloudCostData(topoInfo, cloudTopology, topologyEntityInfoExtractor);
        verifyUptimePercentage(cloudCostData, cloudVm.getOid(), expectedUptime.uptimePercentage());
        verifyUptimePercentage(cloudCostData, cloudVm2.getOid(), defaultUptime.uptimePercentage());

    }

    private void verifyUptimePercentage(CloudCostData data, Long oid, Double expectedUptime) {
        Double actualEntityUptime = data.getEntityUptimePercentage(oid);
        assertEquals(actualEntityUptime, expectedUptime, 0.0001D);

    }
}
