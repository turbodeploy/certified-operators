package com.vmturbo.extractor.topology.fetcher;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.search.metadata.utils.SearchFiltersMapper.SearchFilterSpec;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;

/**
 * Test the supply chain calculation using SupplyChainCalculator and search filters.
 */
public class SupplyChainFetcherTest {

    private final MultiStageTimer multiStageTimer = new MultiStageTimer(null);

    private static final AtomicLong nextId = new AtomicLong(1L);

    /**
     * Test partial supply chain calculation for onprem topology.
     */
    @Test
    public void testOnPrem() {
        final long vm1 = nextId();
        final long vol1 = nextId();
        final long vol2 = nextId();
        final long st1 = nextId();
        final long st2 = nextId();
        final long da1 = nextId();
        final long da2 = nextId();
        final long lp1 = nextId();
        final long pm1 = nextId();
        final long dc1 = nextId();

        TopologyEntityDTO vmDTO1 = EntityBuilder.newBuilder(vm1, EntityType.VIRTUAL_MACHINE)
                .addProviders(st1, st2, pm1).addNormalConnected(vol1, vol2).build();

        TopologyEntityDTO volumeDTO1 = EntityBuilder.newBuilder(vol1, EntityType.VIRTUAL_VOLUME)
                .addNormalConnected(st1).build();
        TopologyEntityDTO volumeDTO2 = EntityBuilder.newBuilder(vol2, EntityType.VIRTUAL_VOLUME)
                .addNormalConnected(st2).build();
        TopologyEntityDTO stDTO1 = EntityBuilder.newBuilder(st1, EntityType.STORAGE).addProviders(lp1).build();
        TopologyEntityDTO lpDTO1 = EntityBuilder.newBuilder(lp1, EntityType.LOGICAL_POOL).addProviders(da1).build();
        TopologyEntityDTO daDTO1 = EntityBuilder.newBuilder(da1, EntityType.DISK_ARRAY).build();

        TopologyEntityDTO stDTO2 = EntityBuilder.newBuilder(st2, EntityType.STORAGE).addProviders(da2).build();
        TopologyEntityDTO daDTO2 = EntityBuilder.newBuilder(da2, EntityType.DISK_ARRAY).build();

        TopologyEntityDTO pmDTO1 = EntityBuilder.newBuilder(pm1, EntityType.PHYSICAL_MACHINE).addProviders(st1, st2, dc1).build();
        TopologyEntityDTO dcDTO = EntityBuilder.newBuilder(dc1, EntityType.DATACENTER).build();

        TopologyGraph<SupplyChainEntity> graph = new TopologyGraphCreator<SupplyChainEntity.Builder, SupplyChainEntity>()
                .addEntities(Stream.of(vmDTO1, volumeDTO1, volumeDTO2, stDTO1, lpDTO1, daDTO1,
                        stDTO2, daDTO2, pmDTO1, dcDTO)
                        .map(SupplyChainEntity::newBuilder)
                        .collect(Collectors.toList()))
                .build();

        // verify partial supply chain calculation got same related entities as full
        verifyPartialSupplyChainCalculationWithFull(graph);
    }

    /**
     * Test partial supply chain calculation for vsan topology.
     */
    @Test
    public void testVSAN() {
        final long vm1 = nextId();
        final long vol1 = nextId();
        final long vsanSt = nextId();
        final long st1 = nextId();
        final long st2 = nextId();
        final long da1 = nextId();
        final long da2 = nextId();
        final long vsanDa = nextId();
        final long pm1 = nextId();
        final long dc1 = nextId();

        TopologyEntityDTO vmDTO1 = EntityBuilder.newBuilder(vm1, EntityType.VIRTUAL_MACHINE)
                .addProviders(vsanSt, pm1).addNormalConnected(vol1).build();
        TopologyEntityDTO volDTO1 = EntityBuilder.newBuilder(vol1, EntityType.VIRTUAL_VOLUME).addNormalConnected(vsanSt).build();
        TopologyEntityDTO vsanStDTO = EntityBuilder.newBuilder(vsanSt, EntityType.STORAGE).addProviders(vsanDa, pm1).build();
        TopologyEntityDTO vsanDaDTO = EntityBuilder.newBuilder(vsanDa, EntityType.DISK_ARRAY).build();

        TopologyEntityDTO pmDTO1 = EntityBuilder.newBuilder(pm1, EntityType.PHYSICAL_MACHINE).addProviders(st1, st2, dc1).build();
        TopologyEntityDTO dcDTO = EntityBuilder.newBuilder(dc1, EntityType.DATACENTER).build();
        TopologyEntityDTO stDTO1 = EntityBuilder.newBuilder(st1, EntityType.STORAGE).addProviders(da1).build();
        TopologyEntityDTO daDTO1 = EntityBuilder.newBuilder(da1, EntityType.DISK_ARRAY).build();
        TopologyEntityDTO stDTO2 = EntityBuilder.newBuilder(st2, EntityType.STORAGE).addProviders(da2).build();
        TopologyEntityDTO daDTO2 = EntityBuilder.newBuilder(da2, EntityType.DISK_ARRAY).build();

        TopologyGraph<SupplyChainEntity> graph = new TopologyGraphCreator<SupplyChainEntity.Builder, SupplyChainEntity>()
                .addEntities(Stream.of(vmDTO1, volDTO1, vsanStDTO, vsanDaDTO, pmDTO1, stDTO1,
                        daDTO1, stDTO2, daDTO2, dcDTO)
                        .map(SupplyChainEntity::newBuilder)
                        .collect(Collectors.toList()))
                .build();

        // verify partial supply chain calculation got same related entities as full
        verifyPartialSupplyChainCalculationWithFull(graph);
    }


    /**
     * Test partial supply chain calculation for cloud topology.
     */
    @Test
    public void testCloud() {
        final long account1 = nextId();
        final long businessApp1 = nextId();
        final long businessTrx1 = nextId();
        final long businessTrx2 = nextId();
        final long service1 = nextId();
        final long app1 = nextId();
        final long vm1 = nextId();
        final long volume1 = nextId();
        final long volume2 = nextId();
        final long stTier1 = nextId();
        final long stTier2 = nextId();
        final long zone1 = nextId();
        final long region1 = nextId();
        final long serviceProvider1 = nextId();

        TopologyEntityDTO accountDTO1 = EntityBuilder.newBuilder(account1, EntityType.BUSINESS_ACCOUNT)
                .addOwns(vm1, volume1, volume2).addAggregatedBy(serviceProvider1).build();
        TopologyEntityDTO businessAppDTO1 = EntityBuilder.newBuilder(businessApp1, EntityType.BUSINESS_APPLICATION)
                .addProviders(businessTrx1, businessTrx2).build();
        TopologyEntityDTO businessTrxDTO1 = EntityBuilder.newBuilder(businessTrx1, EntityType.BUSINESS_TRANSACTION)
                .addProviders(service1).build();
        TopologyEntityDTO businessTrxDTO2 = EntityBuilder.newBuilder(businessTrx2, EntityType.BUSINESS_TRANSACTION)
                .addProviders(service1).build();
        TopologyEntityDTO serviceDTO1 = EntityBuilder.newBuilder(service1, EntityType.SERVICE)
                .addProviders(app1).build();
        TopologyEntityDTO appDTO1 = EntityBuilder.newBuilder(app1, EntityType.APPLICATION_COMPONENT)
                .addProviders(vm1).build();
        TopologyEntityDTO vmDTO1 = EntityBuilder.newBuilder(vm1, EntityType.VIRTUAL_MACHINE)
                .addProviders(volume1, volume2)
                .addAggregatedBy(zone1)
                .build();
        TopologyEntityDTO volumeDTO1 = EntityBuilder.newBuilder(volume1, EntityType.VIRTUAL_VOLUME)
                .addAggregatedBy(zone1).addProviders(stTier1).build();
        TopologyEntityDTO volumeDTO2 = EntityBuilder.newBuilder(volume2, EntityType.VIRTUAL_VOLUME)
                .addAggregatedBy(zone1).addProviders(stTier2).build();
        TopologyEntityDTO stTierDTO1 = EntityBuilder.newBuilder(stTier1, EntityType.STORAGE_TIER)
                .addAggregatedBy(region1).build();
        TopologyEntityDTO stTierDTO2 = EntityBuilder.newBuilder(stTier2, EntityType.STORAGE_TIER)
                .addAggregatedBy(region1).build();
        TopologyEntityDTO zoneDTO1 = EntityBuilder.newBuilder(zone1, EntityType.AVAILABILITY_ZONE).build();
        TopologyEntityDTO regionDTO1 = EntityBuilder.newBuilder(region1, EntityType.REGION)
                .addOwns(zone1).build();
        TopologyEntityDTO serviceProviderDTO1 = EntityBuilder.newBuilder(serviceProvider1, EntityType.SERVICE_PROVIDER)
                .addOwns(region1).build();

        TopologyGraph<SupplyChainEntity> graph = new TopologyGraphCreator<SupplyChainEntity.Builder, SupplyChainEntity>()
                .addEntities(Stream.of(accountDTO1, businessAppDTO1, businessTrxDTO1,
                        businessTrxDTO2, serviceDTO1, appDTO1, vmDTO1, volumeDTO1, volumeDTO2,
                        stTierDTO1, stTierDTO2, zoneDTO1, regionDTO1, serviceProviderDTO1)
                        .map(SupplyChainEntity::newBuilder)
                        .collect(Collectors.toList()))
                .build();

        // verify partial supply chain calculation got same related entities as full
        verifyPartialSupplyChainCalculationWithFull(graph);
    }

    /**
     * Test partial supply chain calculation for cloud native topology.
     */
    @Test
    public void testCloudNative() {
        final long businessApp1 = nextId();
        final long businessTrx1 = nextId();
        final long businessTrx2 = nextId();
        final long service1 = nextId();
        final long app1 = nextId();
        final long container1 = nextId();
        final long containerPod1 = nextId();
        final long containerSpec1 = nextId();
        final long workloadController1 = nextId();
        final long namespace1 = nextId();
        final long cluster1 = nextId();

        final long vm1 = nextId();
        final long vol1 = nextId();
        final long vol2 = nextId();
        final long st1 = nextId();
        final long st2 = nextId();
        final long da1 = nextId();
        final long da2 = nextId();
        final long lp1 = nextId();
        final long pm1 = nextId();
        final long dc1 = nextId();

        TopologyEntityDTO businessAppDTO1 = EntityBuilder.newBuilder(businessApp1, EntityType.BUSINESS_APPLICATION)
                .addProviders(businessTrx1, businessTrx2).build();
        TopologyEntityDTO businessTrxDTO1 = EntityBuilder.newBuilder(businessTrx1, EntityType.BUSINESS_TRANSACTION)
                .addProviders(service1).build();
        TopologyEntityDTO businessTrxDTO2 = EntityBuilder.newBuilder(businessTrx2, EntityType.BUSINESS_TRANSACTION)
                .addProviders(service1).build();
        TopologyEntityDTO serviceDTO1 = EntityBuilder.newBuilder(service1, EntityType.SERVICE)
                .addProviders(app1).build();
        TopologyEntityDTO appDTO1 = EntityBuilder.newBuilder(app1, EntityType.APPLICATION_COMPONENT)
                .addProviders(vm1, container1).build();

        TopologyEntityDTO containerDTO1 = EntityBuilder.newBuilder(container1, EntityType.CONTAINER)
                .addProviders(containerPod1).addAggregatedBy(containerSpec1).build();

        TopologyEntityDTO containerPodDTO1 = EntityBuilder.newBuilder(containerPod1, EntityType.CONTAINER_POD)
                .addProviders(vm1, workloadController1).addAggregatedBy(workloadController1).build();
        TopologyEntityDTO containerSpecDTO1 = EntityBuilder.newBuilder(containerSpec1, EntityType.CONTAINER_SPEC).build();
        TopologyEntityDTO workloadControllerDTO1 = EntityBuilder.newBuilder(workloadController1, EntityType.WORKLOAD_CONTROLLER)
                .addOwns(containerSpec1).addAggregatedBy(namespace1).addProviders(namespace1).build();
        TopologyEntityDTO namespaceDTO1 = EntityBuilder.newBuilder(namespace1,
                EntityType.NAMESPACE).addAggregatedBy(cluster1).build();
        TopologyEntityDTO clusterDTO1 = EntityBuilder.newBuilder(cluster1, EntityType.CONTAINER_PLATFORM_CLUSTER).build();

        TopologyEntityDTO vmDTO1 = EntityBuilder.newBuilder(vm1, EntityType.VIRTUAL_MACHINE)
                .addProviders(st1, st2, pm1).addNormalConnected(vol1, vol2).build();

        TopologyEntityDTO volumeDTO1 = EntityBuilder.newBuilder(vol1, EntityType.VIRTUAL_VOLUME)
                .addNormalConnected(st1).build();
        TopologyEntityDTO volumeDTO2 = EntityBuilder.newBuilder(vol2, EntityType.VIRTUAL_VOLUME)
                .addNormalConnected(st2).build();
        TopologyEntityDTO stDTO1 = EntityBuilder.newBuilder(st1, EntityType.STORAGE).addProviders(lp1).build();
        TopologyEntityDTO lpDTO1 = EntityBuilder.newBuilder(lp1, EntityType.LOGICAL_POOL).addProviders(da1).build();
        TopologyEntityDTO daDTO1 = EntityBuilder.newBuilder(da1, EntityType.DISK_ARRAY).build();

        TopologyEntityDTO stDTO2 = EntityBuilder.newBuilder(st2, EntityType.STORAGE).addProviders(da2).build();
        TopologyEntityDTO daDTO2 = EntityBuilder.newBuilder(da2, EntityType.DISK_ARRAY).build();

        TopologyEntityDTO pmDTO1 = EntityBuilder.newBuilder(pm1, EntityType.PHYSICAL_MACHINE).addProviders(st1, st2, dc1).build();
        TopologyEntityDTO dcDTO = EntityBuilder.newBuilder(dc1, EntityType.DATACENTER).build();

        TopologyGraph<SupplyChainEntity> graph = new TopologyGraphCreator<SupplyChainEntity.Builder, SupplyChainEntity>()
                .addEntities(Stream.of(businessAppDTO1, businessTrxDTO1, businessTrxDTO2,
                        serviceDTO1, appDTO1, containerDTO1, containerPodDTO1, containerSpecDTO1,
                        workloadControllerDTO1, namespaceDTO1, clusterDTO1, vmDTO1, volumeDTO1,
                        volumeDTO2, stDTO1, lpDTO1, daDTO1, stDTO2, daDTO2, pmDTO1, dcDTO)
                        .map(SupplyChainEntity::newBuilder)
                        .collect(Collectors.toList()))
                .build();

        // verify partial supply chain calculation got same related entities as full
        verifyPartialSupplyChainCalculationWithFull(graph);
    }

    /**
     * Verify related entities for each entity in graph, and ensure the related entities calculated
     * through search filters are the same as that through
     * {@link com.vmturbo.topology.graph.supplychain.SupplyChainCalculator}.
     *
     * @param graph topology graph to verify
     */
    private void verifyPartialSupplyChainCalculationWithFull(TopologyGraph<SupplyChainEntity> graph) {
        // calculate both full and partial
        final Map<Long, Map<Integer, Set<Long>>> full = new SupplyChainFetcher(graph,
                multiStageTimer, r -> { }, true).fetch().getEntityToRelatedEntities();
        final Map<Long, Map<Integer, Set<Long>>> partial = new SupplyChainFetcher(graph,
                multiStageTimer, r -> { }, false).fetch().getEntityToRelatedEntities();

        // verify related entities for each entity in graph
        graph.entities().forEach(entity -> {
            assertThat(full.keySet(), hasItem(entity.getOid()));

            Map<Integer, SearchFilterSpec> filterSpecMap =
                    SearchMetadataUtils.SEARCH_FILTER_SPEC_BASED_ON_METADATA.get(
                            entity.getEntityType());
            if (filterSpecMap == null) {
                // no filter spec for given entity type
                // verify that no related entities are calculated for it
                assertThat(partial.keySet(), not(hasItem(entity.getOid())));
                return;
            }

            assertThat(entity.toString(), partial.keySet(), hasItem(entity.getOid()));

            Map<Integer, Set<Long>> fullRelatedEntities = full.get(entity.getOid());
            Map<Integer, Set<Long>> partialRelatedEntities = partial.get(entity.getOid());

            filterSpecMap.keySet().forEach(relatedEntityType -> {
                // compare partial and full
                String errorStr = entity.toString() + ":"
                        + EntityType.forNumber(entity.getEntityType()).name()
                        + "-->" + EntityType.forNumber(relatedEntityType).name();
                assertThat(errorStr, partialRelatedEntities.get(relatedEntityType),
                        equalTo(fullRelatedEntities.get(relatedEntityType)));
            });
        });
    }

    private long nextId() {
        return nextId.getAndIncrement();
    }

    /**
     * Helper builder for {@link TopologyEntityDTO}.
     */
    private static class EntityBuilder {

        private final TopologyEntityDTO.Builder builder;

        private EntityBuilder(TopologyEntityDTO.Builder builder) {
            this.builder = builder;
        }

        public static EntityBuilder newBuilder(long oid, EntityType entityType) {
            return new EntityBuilder(TopologyEntityDTO.newBuilder()
                    .setOid(oid).setEntityType(entityType.getNumber()));
        }

        private EntityBuilder addAggregatedBy(long... aggregatedBy) {
            for (long aggr : aggregatedBy) {
                builder.addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                        .setConnectedEntityId(aggr));
            }
            return this;
        }

        private EntityBuilder addProviders(long... providers) {
            for (long provider : providers) {
                builder.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(provider));
            }
            return this;
        }

        private EntityBuilder addOwns(long... owns) {
            for (long own : owns) {
                builder.addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.OWNS_CONNECTION)
                        .setConnectedEntityId(own));
            }
            return this;
        }

        private EntityBuilder addNormalConnected(long... connects) {
            for (long connect : connects) {
                builder.addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                        .setConnectedEntityId(connect));
            }
            return this;
        }

        public TopologyEntityDTO build() {
            return builder.build();
        }
    }
}
