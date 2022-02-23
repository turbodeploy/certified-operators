package com.vmturbo.cost.component.cloud.commitment;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.cloud.common.commitment.CloudCommitmentTopology;
import com.vmturbo.cloud.common.commitment.CloudCommitmentTopology.CloudCommitmentTopologyFactory;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageVector;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentUtilizationVector;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.ScopedCommitmentCoverage;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.ScopedCommitmentUtilization;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.cloud.commitment.coverage.CoverageInfo;
import com.vmturbo.cost.component.cloud.commitment.mapping.MappingInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;
import com.vmturbo.cost.component.stores.SingleFieldDataStore;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommoditiesBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class for {@link TopologyCommitmentCoverageWriter}.
 */
@RunWith(MockitoJUnitRunner.class)
public class TopologyCommitmentCoverageWriterTest {

    @Mock
    private SingleFieldDataStore<CoverageInfo, ?> commitmentCoverageStore;

    @Mock
    private SingleFieldDataStore<MappingInfo, ?> commitmentMappingStore;

    @Mock
    private SingleFieldDataStore<UtilizationInfo, ?> commitmentUtilizationStore;

    @Mock
    private CloudCommitmentTopologyFactory<TopologyEntityDTO> commitmentTopologyFactory;

    @Mock
    private CloudCommitmentTopology commitmentTopology;

    @Mock
    private CloudTopology<TopologyEntityDTO> cloudTopology;

    private TopologyCommitmentCoverageWriter.Factory coverageWriterFactory;

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(123)
            .build();

    private final TopologyEntityDTO accountEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOid(3L)
            .build();

    private final TopologyEntityDTO regionEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.REGION_VALUE)
            .setOid(4L)
            .build();

    private final TopologyEntityDTO cloudServiceEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CLOUD_SERVICE_VALUE)
            .setOid(5L)
            .build();

    private final TopologyEntityDTO serviceProviderEntity = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.SERVICE_PROVIDER_VALUE)
            .setOid(6L)
            .build();

    private final TopologyEntityDTO virtualMachine1 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(1L)
            .build();

    private final TopologyEntityDTO cloudCommitment = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CLOUD_COMMITMENT_VALUE)
            .setOid(2L)
            .build();

    private final TopologyEntityDTO virtualMachine7 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(7L)
            .build();

    @Before
    public void setup() {
        // set up factories
        when(commitmentTopologyFactory.createTopology(any())).thenReturn(commitmentTopology);

        coverageWriterFactory = new TopologyCommitmentCoverageWriter.Factory(
                commitmentCoverageStore,
                commitmentMappingStore,
                commitmentUtilizationStore,
                commitmentTopologyFactory);
    }

    @Test
    public void testCoveragePersistence() {

        final Table<Long, Long, CloudCommitmentAmount> inputAllocations = ImmutableTable.of(
                1L, 2L, CloudCommitmentAmount.newBuilder()
                        .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                                .addCommodity(CommittedCommodityBought.newBuilder()
                                        .setCommodityType(CommodityType.NUM_VCORE).setCapacity(2))
                                .addCommodity(CommittedCommodityBought.newBuilder()
                                        .setCommodityType(CommodityType.MEM_PROVISIONED).setCapacity(8)))
                        .build());


        // set up the cloud topology
        when(cloudTopology.getEntity(virtualMachine1.getOid())).thenReturn(Optional.of(virtualMachine1));
        when(cloudTopology.getEntity(virtualMachine7.getOid())).thenReturn(Optional.of(virtualMachine7));
        when(cloudTopology.getConnectedAvailabilityZone(anyLong())).thenReturn(Optional.empty());
        when(cloudTopology.getConnectedRegion(anyLong())).thenReturn(Optional.of(regionEntity));
        when(cloudTopology.getOwner(anyLong())).thenReturn(Optional.of(accountEntity));
        when(cloudTopology.getConnectedService(anyLong())).thenReturn(Optional.of(cloudServiceEntity));
        when(cloudTopology.getServiceProvider(anyLong())).thenReturn(Optional.of(serviceProviderEntity));
        when(cloudTopology.getAllEntitiesOfType(EntityType.BUSINESS_ACCOUNT_VALUE)).thenReturn(ImmutableList.of(accountEntity));
        when(cloudTopology.streamOwnedEntitiesOfType(accountEntity.getOid(), EntityType.VIRTUAL_MACHINE_VALUE))
                .thenReturn(Stream.of(virtualMachine1, virtualMachine7));


        // setup commitment capacity
        final CloudCommitmentCoverageTypeInfo vcoreVector = CloudCommitmentCoverageTypeInfo.newBuilder()
                .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                .setCoverageSubtype(CommodityType.NUM_VCORE_VALUE)
                .build();
        final CloudCommitmentCoverageTypeInfo memoryProvisionedVector = CloudCommitmentCoverageTypeInfo.newBuilder()
                .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                .setCoverageSubtype(CommodityType.MEM_PROVISIONED_VALUE)
                .build();
        when(commitmentTopology.getSupportedCoverageVectors(anyLong())).thenReturn(ImmutableSet.of(
                vcoreVector, memoryProvisionedVector));
        when(commitmentTopology.isSupportedAccount(anyLong())).thenReturn(true);
        when(commitmentTopology.getCoverageCapacityForEntity(virtualMachine1.getOid(), vcoreVector)).thenReturn(4.0);
        when(commitmentTopology.getCoverageCapacityForEntity(virtualMachine1.getOid(), memoryProvisionedVector)).thenReturn(12.0);
        when(commitmentTopology.getCoverageCapacityForEntity(virtualMachine7.getOid(), vcoreVector)).thenReturn(8.0);
        when(commitmentTopology.getCoverageCapacityForEntity(virtualMachine7.getOid(), memoryProvisionedVector)).thenReturn(16.0);

        // invoke the writer
        final TopologyCommitmentCoverageWriter coverageWriter = coverageWriterFactory.newWriter(cloudTopology);
        coverageWriter.persistCommitmentAllocations(topologyInfo, inputAllocations);

        // verify persistence to coverage store
        final ArgumentCaptor<CoverageInfo> coverageInfoCaptor = ArgumentCaptor.forClass(CoverageInfo.class);
        verify(commitmentCoverageStore).setData(coverageInfoCaptor.capture());

        // Assertions
        final CoverageInfo actualCoverageInfo = coverageInfoCaptor.getValue();
        assertThat(actualCoverageInfo.topologyInfo(), equalTo(topologyInfo));

        // set up the expected coverage map for VM 1 and 7
        final ScopedCommitmentCoverage expectedFirstScope = ScopedCommitmentCoverage.newBuilder()
                .setEntityOid(virtualMachine1.getOid())
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setAccountOid(accountEntity.getOid())
                .setRegionOid(regionEntity.getOid())
                .setCloudServiceOid(cloudServiceEntity.getOid())
                .setServiceProviderOid(serviceProviderEntity.getOid())
                .build();
        final CloudCommitmentCoverageVector expectedFirstCoreCoverage = CloudCommitmentCoverageVector.newBuilder()
                .setVectorType(vcoreVector)
                .setUsed(2.0)
                .setCapacity(4.0)
                .build();
        final CloudCommitmentCoverageVector expectedFirstMemoryCoverage = CloudCommitmentCoverageVector.newBuilder()
                .setVectorType(memoryProvisionedVector)
                .setUsed(8.0)
                .setCapacity(12.0)
                .build();

        final ScopedCommitmentCoverage expectedSecondScope = ScopedCommitmentCoverage.newBuilder()
                .setEntityOid(virtualMachine7.getOid())
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setAccountOid(accountEntity.getOid())
                .setRegionOid(regionEntity.getOid())
                .setCloudServiceOid(cloudServiceEntity.getOid())
                .setServiceProviderOid(serviceProviderEntity.getOid())
                .build();
        final CloudCommitmentCoverageVector expectedSecondCoreCoverage = CloudCommitmentCoverageVector.newBuilder()
                .setVectorType(vcoreVector)
                .setUsed(0.0)
                .setCapacity(8.0)
                .build();
        final CloudCommitmentCoverageVector expectedSecondMemoryCoverage = CloudCommitmentCoverageVector.newBuilder()
                .setVectorType(memoryProvisionedVector)
                .setUsed(0.0)
                .setCapacity(16.0)
                .build();

        assertThat(actualCoverageInfo.entityCoverageMap(), hasKey(expectedFirstScope.getEntityOid()));
        final ScopedCommitmentCoverage actualFirstCoverage = actualCoverageInfo.entityCoverageMap().get(expectedFirstScope.getEntityOid());
        assertThat(actualFirstCoverage.toBuilder().clearCoverageVector().build(), equalTo(expectedFirstScope));
        assertThat(actualFirstCoverage.getCoverageVectorList(), containsInAnyOrder(expectedFirstCoreCoverage, expectedFirstMemoryCoverage));

        assertThat(actualCoverageInfo.entityCoverageMap(), hasKey(expectedSecondScope.getEntityOid()));
        final ScopedCommitmentCoverage actualSecondCoverage = actualCoverageInfo.entityCoverageMap().get(expectedSecondScope.getEntityOid());
        assertThat(actualSecondCoverage.toBuilder().clearCoverageVector().build(), equalTo(expectedSecondScope));
        assertThat(actualSecondCoverage.getCoverageVectorList(), containsInAnyOrder(expectedSecondCoreCoverage, expectedSecondMemoryCoverage));
    }

    @Test
    public void testMappingPersistence() {

        final Table<Long, Long, CloudCommitmentAmount> inputAllocations = ImmutableTable.of(
                1L, 2L, CloudCommitmentAmount.newBuilder()
                        .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                                .addCommodity(CommittedCommodityBought.newBuilder()
                                        .setCommodityType(CommodityType.NUM_VCORE).setCapacity(2))
                                .addCommodity(CommittedCommodityBought.newBuilder()
                                        .setCommodityType(CommodityType.MEM_PROVISIONED).setCapacity(8)))
                        .build());

        // invoke the writer
        final TopologyCommitmentCoverageWriter coverageWriter = coverageWriterFactory.newWriter(cloudTopology);
        coverageWriter.persistCommitmentAllocations(topologyInfo, inputAllocations);

        // verify mapping store
        final ArgumentCaptor<MappingInfo> mappingInfoCaptor = ArgumentCaptor.forClass(MappingInfo.class);
        verify(commitmentMappingStore).setData(mappingInfoCaptor.capture());

        // assertions
        final MappingInfo actualMappingInfo = mappingInfoCaptor.getValue();
        assertThat(actualMappingInfo.topologyInfo(), equalTo(topologyInfo));

        final CloudCommitmentMapping expectedMapping = CloudCommitmentMapping.newBuilder()
                .setEntityOid(1)
                .setCloudCommitmentOid(2)
                .setCommitmentAmount(Iterables.getOnlyElement(inputAllocations.values()))
                .build();
        assertThat(actualMappingInfo.cloudCommitmentMappings(), hasSize(1));
        assertThat(actualMappingInfo.cloudCommitmentMappings().get(0), equalTo(expectedMapping));
    }

    @Test
    public void testUtilizationPersistence() {

        final Table<Long, Long, CloudCommitmentAmount> inputAllocations = ImmutableTable.<Long, Long, CloudCommitmentAmount>builder()
                .put(1L, 2L, CloudCommitmentAmount.newBuilder()
                        .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                                .addCommodity(CommittedCommodityBought.newBuilder()
                                        .setCommodityType(CommodityType.NUM_VCORE)
                                        .setCapacity(2))
                                .addCommodity(CommittedCommodityBought.newBuilder()
                                        .setCommodityType(CommodityType.MEM_PROVISIONED)
                                        .setCapacity(8)))
                        .build())
                .put(7L, 2L, CloudCommitmentAmount.newBuilder()
                        .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                                .addCommodity(CommittedCommodityBought.newBuilder()
                                        .setCommodityType(CommodityType.NUM_VCORE)
                                        .setCapacity(3))
                                .addCommodity(CommittedCommodityBought.newBuilder()
                                        .setCommodityType(CommodityType.MEM_PROVISIONED)
                                        .setCapacity(5)))
                        .build())
                .build();

        // set up the cloud topology
        when(cloudTopology.getEntity(cloudCommitment.getOid())).thenReturn(Optional.of(cloudCommitment));
        when(cloudTopology.getConnectedRegion(anyLong())).thenReturn(Optional.of(regionEntity));
        when(cloudTopology.getOwner(anyLong())).thenReturn(Optional.of(accountEntity));
        when(cloudTopology.getServiceProvider(anyLong())).thenReturn(Optional.of(serviceProviderEntity));
        when(cloudTopology.getAllEntitiesOfType(EntityType.CLOUD_COMMITMENT_VALUE)).thenReturn(ImmutableList.of(cloudCommitment));

        // set up the commitment topology
        when(commitmentTopology.isSupportedAccount(anyLong())).thenReturn(true);

        // setup commitment capacity
        final CloudCommitmentCoverageTypeInfo coreVector = CloudCommitmentCoverageTypeInfo.newBuilder()
                .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                .setCoverageSubtype(CommodityType.NUM_VCORE_VALUE)
                .build();
        final CloudCommitmentCoverageTypeInfo memoryVector = CloudCommitmentCoverageTypeInfo.newBuilder()
                .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                .setCoverageSubtype(CommodityType.MEM_PROVISIONED_VALUE)
                .build();
        when(commitmentTopology.getCommitmentCapacityVectors(cloudCommitment.getOid()))
                .thenReturn(ImmutableMap.of(
                        coreVector, 32.0,
                        memoryVector, 64.0));

        // invoke the writer
        final TopologyCommitmentCoverageWriter coverageWriter = coverageWriterFactory.newWriter(cloudTopology);
        coverageWriter.persistCommitmentAllocations(topologyInfo, inputAllocations);

        // Verify utilization store
        final ArgumentCaptor<UtilizationInfo> utilizationInfoCaptor = ArgumentCaptor.forClass(UtilizationInfo.class);
        verify(commitmentUtilizationStore).setData(utilizationInfoCaptor.capture());

        // Assertions
        final UtilizationInfo actualUtilizationInfo = utilizationInfoCaptor.getValue();
        assertThat(actualUtilizationInfo.topologyInfo(), equalTo(topologyInfo));

        // set up expected utilization map
        final ScopedCommitmentUtilization expectedUtilizationScope = ScopedCommitmentUtilization.newBuilder()
                        .setCloudCommitmentOid(cloudCommitment.getOid())
                        .setAccountOid(accountEntity.getOid())
                        .setRegionOid(regionEntity.getOid())
                        .setServiceProviderOid(serviceProviderEntity.getOid())
                        .build();
        final CloudCommitmentUtilizationVector expectedCoreVector = CloudCommitmentUtilizationVector.newBuilder()
                .setVectorType(coreVector)
                .setUsed(5.0)
                .setCapacity(32.0)
                .build();

        final CloudCommitmentUtilizationVector expectedMemoryVector = CloudCommitmentUtilizationVector.newBuilder()
                .setVectorType(memoryVector)
                .setUsed(13.0)
                .setCapacity(64.0)
                .build();

        assertThat(actualUtilizationInfo.commitmentUtilizationMap(), hasKey(cloudCommitment.getOid()));

        final ScopedCommitmentUtilization actualUtilization = actualUtilizationInfo.commitmentUtilizationMap().get(cloudCommitment.getOid());
        assertThat(actualUtilization.toBuilder().clearUtilizationVector().build(), equalTo(expectedUtilizationScope));
        assertThat(actualUtilization.getUtilizationVectorList(), containsInAnyOrder(expectedCoreVector, expectedMemoryVector));
    }
}
