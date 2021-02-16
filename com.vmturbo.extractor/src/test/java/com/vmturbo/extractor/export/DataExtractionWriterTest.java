package com.vmturbo.extractor.export;

import static com.vmturbo.common.protobuf.topology.TopologyDTOUtil.QX_VCPU_BASE_COEFFICIENT;
import static com.vmturbo.extractor.util.TopologyTestUtil.boughtCommoditiesFromProvider;
import static com.vmturbo.extractor.util.TopologyTestUtil.mkEntity;
import static com.vmturbo.extractor.util.TopologyTestUtil.soldCommodities;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.CPU;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.MEM;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.Q1_VCPU;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.Q2_VCPU;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.STORAGE_ACCESS;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.STORAGE_AMOUNT;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VMEM;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.javatuples.Quartet;
import org.javatuples.Triplet;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.schema.json.export.Entity;
import com.vmturbo.extractor.schema.json.export.ExportedObject;
import com.vmturbo.extractor.schema.json.export.RelatedEntity;
import com.vmturbo.extractor.search.EnumUtils.EntityStateUtils;
import com.vmturbo.extractor.search.EnumUtils.EnvironmentTypeUtils;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.GroupFetcher.GroupData;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
import com.vmturbo.extractor.util.ExtractorTestUtil;
import com.vmturbo.extractor.util.TopologyTestUtil;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Test that data extraction for entities works.
 */
public class DataExtractionWriterTest {

    private static final Grouping CLUSTER1 = Grouping.newBuilder()
            .setId(1234)
            .setDefinition(GroupDefinition.newBuilder()
                    .setType(GroupType.COMPUTE_HOST_CLUSTER)
                    .setDisplayName("cluster1"))
            .build();
    private static final Grouping STORAGE_CLUSTER1 = Grouping.newBuilder()
            .setId(1235)
            .setDefinition(GroupDefinition.newBuilder()
                    .setType(GroupType.STORAGE_CLUSTER)
                    .setDisplayName("storageCluster1"))
            .build();

    private static final TopologyInfo info = TopologyTestUtil.mkRealtimeTopologyInfo(1L);
    private static final MultiStageTimer timer = mock(MultiStageTimer.class);
    private final DataProvider dataProvider = mock(DataProvider.class);
    private final SupplyChain supplyChain = mock(SupplyChain.class);
    private final GroupData groupData = mock(GroupData.class);
    private final TopologyGraph<SupplyChainEntity> topologyGraph = mock(TopologyGraph.class);
    private final ExtractorKafkaSender extractorKafkaSender = mock(ExtractorKafkaSender.class);
    private final DataExtractionFactory dataExtractionFactory = new DataExtractionFactory();
    private DataExtractionWriter writer;
    private List<Entity> entitiesCapture;

    /**
     * Setup before each test.
     *
     * @throws Exception any error happens
     */
    @Before
    public void setUp() throws Exception {
        // mock
        when(dataProvider.getSupplyChain()).thenReturn(supplyChain);
        when(dataProvider.getGroupData()).thenReturn(groupData);
        when(dataProvider.getTopologyGraph()).thenReturn(topologyGraph);
        // capture entities sent to kafka
        this.entitiesCapture = new ArrayList<>();
        doAnswer(inv -> {
            Collection<ExportedObject> exportedObjects = inv.getArgumentAt(0, Collection.class);
            if (exportedObjects != null) {
                entitiesCapture.addAll(exportedObjects.stream()
                        .map(ExportedObject::getEntity)
                        .collect(Collectors.toList()));
            }
            return null;
        }).when(extractorKafkaSender).send(any());
        this.writer = spy(new DataExtractionWriter(extractorKafkaSender, dataExtractionFactory));
        writer.startTopology(info, ExtractorTestUtil.config, timer);
    }

    /**
     * Test that entities data are extracted and sent to kafka correctly.
     * In the test topology:
     *     a vm buys from one host (pm) and two storages (st1 and st2)
     * In supply chain:
     *     vm is connected to pm, st1, st2
     *     pm is connected to vm, st1
     *     st2 is connected to vm (not pm)
     * In group:
     *    pm is member of Cluster cluster1
     *    st1 and st2 are members of StorageCluster storageCluster1
     * It tests the following cases:
     *    basic fields
     *    type specific info is correct
     *    bought/sold commodities are extracted
     *    related entities and related groups (like pm, st1, st2, cluster1, storageCluster1 are in vm's related)
     */
    @Test
    public void testEntityExtraction() {
        final TopologyEntityDTO st1 = mkEntity(STORAGE).toBuilder()
                .addAllCommoditySoldList(soldCommodities(
                        Quartet.with(STORAGE_ACCESS, "", 2500.0, 5000.0),
                        Quartet.with(STORAGE_AMOUNT, "", 2048.0, 4096.0)
                )).build();
        final TopologyEntityDTO st2 = mkEntity(STORAGE).toBuilder()
                .addAllCommoditySoldList(soldCommodities(
                        Quartet.with(STORAGE_ACCESS, "", 2000.0, 4000.0),
                        Quartet.with(STORAGE_AMOUNT, "", 4096.0, 5120.0)
                )).build();
        final TopologyEntityDTO pm = mkEntity(PHYSICAL_MACHINE).toBuilder()
                .addAllCommoditySoldList(soldCommodities(
                        Quartet.with(CPU, "a", 2000.0, 10000.0),
                        Quartet.with(MEM, "b", 1024.0, 2048.0),
                        Quartet.with(Q1_VCPU, "", 200.0, QX_VCPU_BASE_COEFFICIENT),
                        Quartet.with(Q2_VCPU, "", 400.0, QX_VCPU_BASE_COEFFICIENT)
                )).build();
        final TopologyEntityDTO vm = mkEntity(VIRTUAL_MACHINE).toBuilder()
                .addAllCommoditySoldList(soldCommodities(
                        Quartet.with(VCPU, "", 200.0, 500.0), Quartet.with(VMEM, "", 512.0, 1024.0)
                ))
                .addCommoditiesBoughtFromProviders(boughtCommoditiesFromProvider(pm,
                        Triplet.with(CPU, "", 200.0), Triplet.with(MEM, "", 512.0),
                        Triplet.with(Q1_VCPU, "", 100.0)
                ))
                .addCommoditiesBoughtFromProviders(boughtCommoditiesFromProvider(st1,
                        Triplet.with(STORAGE_ACCESS, "", 400.0),
                        Triplet.with(STORAGE_AMOUNT, "", 512.0)
                ))
                .addCommoditiesBoughtFromProviders(boughtCommoditiesFromProvider(st2,
                        Triplet.with(STORAGE_ACCESS, "", 300.0),
                        Triplet.with(STORAGE_AMOUNT, "", 1024.0)
                ))
                .setTags(Tags.newBuilder()
                        .putTags("foo", TagValuesDTO.newBuilder().addValues("a").build())
                        .putTags("bar", TagValuesDTO.newBuilder().addValues("b").addValues("c").build()))
                .build();

        // mock supply chain
        doReturn(ImmutableMap.of(
                STORAGE.getNumber(), ImmutableSet.of(st1.getOid(), st2.getOid()),
                PHYSICAL_MACHINE.getNumber(), ImmutableSet.of(pm.getOid()))
        ).when(supplyChain).getRelatedEntities(vm.getOid());
        doReturn(ImmutableMap.of(
                STORAGE.getNumber(), ImmutableSet.of(st1.getOid()),
                VIRTUAL_MACHINE.getNumber(), ImmutableSet.of(vm.getOid()))
        ).when(supplyChain).getRelatedEntities(pm.getOid());
        doReturn(ImmutableMap.of(
                VIRTUAL_MACHINE.getNumber(), ImmutableSet.of(vm.getOid()),
                PHYSICAL_MACHINE.getNumber(), ImmutableSet.of(pm.getOid()))
        ).when(supplyChain).getRelatedEntities(st1.getOid());
        doReturn(ImmutableMap.of(
                VIRTUAL_MACHINE.getNumber(), ImmutableSet.of(vm.getOid()))
        ).when(supplyChain).getRelatedEntities(st2.getOid());
        // mock graph
        doReturn(Optional.of(new SupplyChainEntity(vm))).when(topologyGraph).getEntity(vm.getOid());
        doReturn(Optional.of(new SupplyChainEntity(pm))).when(topologyGraph).getEntity(pm.getOid());
        doReturn(Optional.of(new SupplyChainEntity(st1))).when(topologyGraph).getEntity(st1.getOid());
        doReturn(Optional.of(new SupplyChainEntity(st2))).when(topologyGraph).getEntity(st2.getOid());
        // mock group
        doReturn(ImmutableList.of(CLUSTER1)).when(groupData).getGroupsForEntity(vm.getOid());
        doReturn(ImmutableList.of(CLUSTER1)).when(groupData).getGroupsForEntity(pm.getOid());
        doReturn(ImmutableList.of(STORAGE_CLUSTER1)).when(groupData).getGroupsForEntity(st1.getOid());
        doReturn(ImmutableList.of(STORAGE_CLUSTER1)).when(groupData).getGroupsForEntity(st2.getOid());

        // write all entities
        writer.writeEntity(vm);
        writer.writeEntity(pm);
        writer.writeEntity(st1);
        writer.writeEntity(st2);
        writer.finish(dataProvider);

        // verify 4 entities are sent to kafka
        assertThat(entitiesCapture.size(), is(4));

        final Map<Long, Entity> entityMap = entitiesCapture.stream()
                .collect(Collectors.toMap(Entity::getOid, e -> e));
        final Entity vmEntity = entityMap.get(vm.getOid());
        final Entity pmEntity = entityMap.get(pm.getOid());
        final Entity stEntity1 = entityMap.get(st1.getOid());
        final Entity stEntity2 = entityMap.get(st2.getOid());

        // verify basic fields
        verifyBasicFields(vmEntity, vm);
        verifyBasicFields(pmEntity, pm);
        verifyBasicFields(stEntity1, st1);
        verifyBasicFields(stEntity2, st2);

        // verify type specific info
        Map<String, Object> vmAttrs = vmEntity.getAttrs();
        assertThat(vmAttrs.size(), is(4));
        assertThat(vmAttrs.get("num_cpus"), is(12));
        assertThat(vmAttrs.get("guest_os_type"), is(OSType.LINUX));
        assertThat(vmAttrs.get("connected_networks"), is(Lists.newArrayList("net1")));
        assertThat(vmAttrs.get(ExportUtils.TAGS_JSON_KEY_NAME), is(ImmutableMap.of(
                "foo", Lists.newArrayList("a"),
                "bar", Lists.newArrayList("b", "c")
        )));

        Map<String, Object> pmAttrs = pmEntity.getAttrs();
        assertThat(pmAttrs.size(), is(4));
        assertThat(pmAttrs.get("num_cpus"), is(12));
        assertThat(pmAttrs.get("cpu_model"), is("XXX"));
        assertThat(pmAttrs.get("timezone"), is("UTC"));
        assertThat(pmAttrs.get("model"), is("zcvzxv"));

        Map<String, Object> stAttrs1 = stEntity1.getAttrs();
        assertThat(stAttrs1.size(), is(1));
        assertThat(stAttrs1.get("is_local"), is(true));

        Map<String, Object> stAttrs2 = stEntity2.getAttrs();
        assertThat(stAttrs2.size(), is(1));
        assertThat(stAttrs1.get("is_local"), is(true));

        // verify commodities
        // vm sold
        assertThat(vmEntity.getMetric().size(), is(7));
        assertThat(vmEntity.getMetric().get(MetricType.VMEM.getLiteral()).getCurrent(), is(512.0));
        assertThat(vmEntity.getMetric().get(MetricType.VMEM.getLiteral()).getCapacity(), is(1024.0));
        assertThat(vmEntity.getMetric().get(MetricType.VMEM.getLiteral()).getUtilization(), is(0.5));
        assertThat(vmEntity.getMetric().get(MetricType.VCPU.getLiteral()).getCurrent(), is(200.0));
        assertThat(vmEntity.getMetric().get(MetricType.VCPU.getLiteral()).getCapacity(), is(500.0));
        assertThat(vmEntity.getMetric().get(MetricType.VCPU.getLiteral()).getUtilization(), is(0.4));
        // Q1_VCPU is converted to sold
        assertThat(vmEntity.getMetric().get(MetricType.CPU_READY.getLiteral()).getCurrent(), is(100.0));
        assertThat(vmEntity.getMetric().get(MetricType.CPU_READY.getLiteral()).getCapacity(), is(20000.0));
        assertThat(vmEntity.getMetric().get(MetricType.CPU_READY.getLiteral()).getUtilization(), is(0.005));
        // vm bought
        assertThat(vmEntity.getMetric().get(MetricType.CPU.getLiteral()).getConsumed(), is(200.0));
        assertThat(vmEntity.getMetric().get(MetricType.MEM.getLiteral()).getConsumed(), is(512.0));
        // sum of all providers
        assertThat(vmEntity.getMetric().get(MetricType.STORAGE_ACCESS.getLiteral()).getConsumed(), is(700.0));
        assertThat(vmEntity.getMetric().get(MetricType.STORAGE_AMOUNT.getLiteral()).getConsumed(), is(1536.0));

        // pm sold
        assertThat(pmEntity.getMetric().size(), is(4));
        assertThat(pmEntity.getMetric().get(MetricType.CPU.getLiteral()).getCurrent(), is(2000.0));
        assertThat(pmEntity.getMetric().get(MetricType.CPU.getLiteral()).getCapacity(), is(10000.0));
        assertThat(pmEntity.getMetric().get(MetricType.CPU.getLiteral()).getUtilization(), is(0.2));
        assertThat(pmEntity.getMetric().get(MetricType.MEM.getLiteral()).getCurrent(), is(1024.0));
        assertThat(pmEntity.getMetric().get(MetricType.MEM.getLiteral()).getCapacity(), is(2048.0));
        assertThat(pmEntity.getMetric().get(MetricType.MEM.getLiteral()).getUtilization(), is(0.5));
        assertThat(pmEntity.getMetric().get(MetricType.Q1_VCPU.getLiteral()).getCurrent(), is(200.0));
        assertThat(pmEntity.getMetric().get(MetricType.Q1_VCPU.getLiteral()).getCapacity(), is(20000.0));
        assertThat(pmEntity.getMetric().get(MetricType.Q1_VCPU.getLiteral()).getUtilization(), is(0.01));
        assertThat(pmEntity.getMetric().get(MetricType.Q2_VCPU.getLiteral()).getCurrent(), is(400.0));
        assertThat(pmEntity.getMetric().get(MetricType.Q2_VCPU.getLiteral()).getCapacity(), is(20000.0));
        assertThat(pmEntity.getMetric().get(MetricType.Q2_VCPU.getLiteral()).getUtilization(), is(0.02));

        // st1 sold
        assertThat(stEntity1.getMetric().size(), is(2));
        assertThat(stEntity1.getMetric().get(MetricType.STORAGE_ACCESS.getLiteral()).getCurrent(), is(2500.0));
        assertThat(stEntity1.getMetric().get(MetricType.STORAGE_ACCESS.getLiteral()).getCapacity(), is(5000.0));
        assertThat(stEntity1.getMetric().get(MetricType.STORAGE_ACCESS.getLiteral()).getUtilization(), is(0.5));
        assertThat(stEntity1.getMetric().get(MetricType.STORAGE_AMOUNT.getLiteral()).getCurrent(), is(2048.0));
        assertThat(stEntity1.getMetric().get(MetricType.STORAGE_AMOUNT.getLiteral()).getCapacity(), is(4096.0));
        assertThat(stEntity1.getMetric().get(MetricType.STORAGE_AMOUNT.getLiteral()).getUtilization(), is(0.5));

        // st2 sold
        assertThat(stEntity2.getMetric().size(), is(2));
        assertThat(stEntity2.getMetric().get(MetricType.STORAGE_ACCESS.getLiteral()).getCurrent(), is(2000.0));
        assertThat(stEntity2.getMetric().get(MetricType.STORAGE_ACCESS.getLiteral()).getCapacity(), is(4000.0));
        assertThat(stEntity2.getMetric().get(MetricType.STORAGE_ACCESS.getLiteral()).getUtilization(), is(0.5));
        assertThat(stEntity2.getMetric().get(MetricType.STORAGE_AMOUNT.getLiteral()).getCurrent(), is(4096.0));
        assertThat(stEntity2.getMetric().get(MetricType.STORAGE_AMOUNT.getLiteral()).getCapacity(), is(5120.0));
        assertThat(stEntity2.getMetric().get(MetricType.STORAGE_AMOUNT.getLiteral()).getUtilization(), is(0.8));

        // verify related entities
        assertThat(vmEntity.getRelated().size(), is(4));
        assertThat(getRelatedEntityIds(vmEntity, EntityType.PHYSICAL_MACHINE), containsInAnyOrder(pm.getOid()));
        assertThat(getRelatedEntityIds(vmEntity, EntityType.STORAGE), containsInAnyOrder(st1.getOid(), st2.getOid()));
        assertThat(getRelatedEntityIds(vmEntity, EntityType.COMPUTE_CLUSTER), containsInAnyOrder(CLUSTER1.getId()));
        assertThat(getRelatedEntityIds(vmEntity, EntityType.STORAGE_CLUSTER), containsInAnyOrder(STORAGE_CLUSTER1.getId()));

        assertThat(pmEntity.getRelated().size(), is(4));
        assertThat(getRelatedEntityIds(pmEntity, EntityType.VIRTUAL_MACHINE), containsInAnyOrder(vm.getOid()));
        assertThat(getRelatedEntityIds(pmEntity, EntityType.STORAGE), containsInAnyOrder(st1.getOid()));
        assertThat(getRelatedEntityIds(pmEntity, EntityType.COMPUTE_CLUSTER), containsInAnyOrder(CLUSTER1.getId()));
        assertThat(getRelatedEntityIds(pmEntity, EntityType.STORAGE_CLUSTER), containsInAnyOrder(STORAGE_CLUSTER1.getId()));

        assertThat(stEntity1.getRelated().size(), is(4));
        assertThat(getRelatedEntityIds(stEntity1, EntityType.VIRTUAL_MACHINE), containsInAnyOrder(vm.getOid()));
        assertThat(getRelatedEntityIds(stEntity1, EntityType.PHYSICAL_MACHINE), containsInAnyOrder(pm.getOid()));
        assertThat(getRelatedEntityIds(stEntity1, EntityType.COMPUTE_CLUSTER), containsInAnyOrder(CLUSTER1.getId()));
        assertThat(getRelatedEntityIds(stEntity1, EntityType.STORAGE_CLUSTER), containsInAnyOrder(STORAGE_CLUSTER1.getId()));

        assertThat(stEntity2.getRelated().size(), is(2));
        assertThat(getRelatedEntityIds(stEntity2, EntityType.VIRTUAL_MACHINE), containsInAnyOrder(vm.getOid()));
        assertThat(getRelatedEntityIds(stEntity2, EntityType.STORAGE_CLUSTER), containsInAnyOrder(STORAGE_CLUSTER1.getId()));
    }

    /**
     * Verify that the basic fields in entity is same as that in topologyEntityDTO.
     *
     * @param entity entity after data extraction
     * @param e source TopologyEntityDTO
     */
    private static void verifyBasicFields(Entity entity, TopologyEntityDTO e) {
        assertThat(entity.getName(), is(e.getDisplayName()));
        assertThat(entity.getType(), is(ExportUtils.getEntityTypeJsonKey(e.getEntityType())));
        assertThat(entity.getEnvironment(), is(EnvironmentTypeUtils.protoToDb(e.getEnvironmentType()).getLiteral()));
        assertThat(entity.getState(), is(EntityStateUtils.protoToDb(e.getEntityState()).getLiteral()));
    }

    /**
     * Get ids of related entities from given entity.
     *
     * @param entity Entity
     * @param relatedEntityType type of related entity
     * @return list of related entity ids
     */
    private static List<Long> getRelatedEntityIds(Entity entity, EntityType relatedEntityType) {
        return entity.getRelated().get(relatedEntityType.getLiteral()).stream()
                .map(RelatedEntity::getOid)
                .collect(Collectors.toList());
    }
}