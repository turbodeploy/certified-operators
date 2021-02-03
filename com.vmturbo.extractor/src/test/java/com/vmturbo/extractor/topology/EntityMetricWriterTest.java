package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CAPACITY;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CONSUMED;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CURRENT;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_PROVIDER;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_TYPE;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_UTILIZATION;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_HASH;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.FILE_PATH;
import static com.vmturbo.extractor.models.ModelDefinitions.FILE_SIZE;
import static com.vmturbo.extractor.models.ModelDefinitions.MODIFICATION_TIME;
import static com.vmturbo.extractor.models.ModelDefinitions.STORAGE_NAME;
import static com.vmturbo.extractor.models.ModelDefinitions.STORAGE_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.TIME;
import static com.vmturbo.extractor.topology.EntityMetricWriter.VM_QX_VCPU_NAME;
import static com.vmturbo.extractor.util.RecordTestUtil.MapMatchesLaxly.mapMatchesLaxly;
import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static com.vmturbo.extractor.util.RecordTestUtil.createMetricRecordMap;
import static com.vmturbo.extractor.util.TopologyTestUtil.boughtCommoditiesFromProvider;
import static com.vmturbo.extractor.util.TopologyTestUtil.cloudVolume;
import static com.vmturbo.extractor.util.TopologyTestUtil.file;
import static com.vmturbo.extractor.util.TopologyTestUtil.mkEntity;
import static com.vmturbo.extractor.util.TopologyTestUtil.onPremVolume;
import static com.vmturbo.extractor.util.TopologyTestUtil.soldCommodities;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.CPU;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.CPU_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.MEM;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.MEM_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE_TIER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineFileType.CONFIGURATION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineFileType.DISK;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineFileType.ISO;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineFileType.LOG;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineFileType.SWAP;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;

import org.javatuples.Quartet;
import org.javatuples.Triplet;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslUpdateRecordSink;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.topology.ImmutableWriterConfig.Builder;
import com.vmturbo.extractor.util.ExtractorTestUtil;
import com.vmturbo.extractor.util.ExtractorTestUtil.EntitiesProcessor;
import com.vmturbo.extractor.util.TopologyTestUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Tests of EntityMetricWriter.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
public class EntityMetricWriterTest {

    @Autowired
    private ExtractorDbConfig dbConfig;

    private EntityMetricWriter writer;
    final TopologyInfo info = TopologyTestUtil.mkRealtimeTopologyInfo(1L);
    final MultiStageTimer timer = mock(MultiStageTimer.class);
    private final DataProvider dataProvider = mock(DataProvider.class);
    private List<Record> entitiesUpsertCapture;
    private List<Record> entitiesUpdateCapture;
    private List<Record> metricInsertCapture;
    private List<Record> wastedFileReplacerCapture;
    private WriterConfig config;

    /**
     * Set up for tests.
     *
     * <p>We create a mock DSLContext that won't do anything, and we also set up record-capturing
     * record sinks and arrange for our test writer instance to use them, so we can verify that the
     * records written by the writer are correct.</p>
     *
     * @throws UnsupportedDialectException if our db endpoint is misconfigured
     * @throws SQLException                if there's a DB issue
     * @throws InterruptedException        if interrupted
     */
    @Before
    public void before() throws UnsupportedDialectException, SQLException, InterruptedException {
        // any test method can safely call this method with customized commodity configs needed
        // for that test. Otherwise the built-in production defaults will be used.
        setupWriterAndSinks(null, (Multimap<CommodityType, EntityType>)null);
    }

    private void setupWriterAndSinks(
            List<Integer> commodityWhitelist,
            Map<CommodityType, EntityType> unaggregatedCommodityTypeMap)
            throws UnsupportedDialectException, InterruptedException, SQLException {
        setupWriterAndSinks(commodityWhitelist,
                ImmutableSetMultimap.copyOf(unaggregatedCommodityTypeMap.entrySet()));
    }

    private void setupWriterAndSinks(
            List<Integer> commodityWhitelist,
            Multimap<CommodityType, EntityType> unaggregatedCommodityTypes)
            throws UnsupportedDialectException, InterruptedException, SQLException {
        config = getWriteConfig(commodityWhitelist, unaggregatedCommodityTypes);
        final DbEndpoint endpoint = spy(dbConfig.ingesterEndpoint());
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        DslRecordSink entitiesUpserterSink = mock(DslUpsertRecordSink.class);
        this.entitiesUpsertCapture = captureSink(entitiesUpserterSink, false);
        DslRecordSink entitiesUpdaterSink = mock(DslUpdateRecordSink.class);
        this.entitiesUpdateCapture = captureSink(entitiesUpdaterSink, false);
        DslRecordSink metricInserterSink = mock(DslRecordSink.class);
        this.metricInsertCapture = captureSink(metricInserterSink, false);
        DslRecordSink wastedFileReplacerSink = mock(DslRecordSink.class);
        this.wastedFileReplacerCapture = captureSink(wastedFileReplacerSink, false);
        final EntityIdManager entityIdManager = new EntityIdManager();
        this.writer = spy(new EntityMetricWriter(endpoint, new EntityHashManager(config),
                mock(ScopeManager.class), entityIdManager,
                Executors.newSingleThreadScheduledExecutor()));
        doReturn(entitiesUpserterSink).when(writer).getEntityUpsertSink(
                any(DSLContext.class), any(), any());
        doReturn(entitiesUpdaterSink).when(writer).getEntityUpdaterSink(
                any(DSLContext.class), any(), any(), any());
        doReturn(metricInserterSink).when(writer).getMetricInserterSink(any(DSLContext.class));
        doReturn(Stream.empty()).when(dataProvider).getAllGroups();
        doReturn(wastedFileReplacerSink).when(writer).getWastedFileReplacerSink(any(DSLContext.class));
    }

    private WriterConfig getWriteConfig(final List<Integer> commodityWhitelist,
            final Multimap<CommodityType, EntityType> unaggregatedCommodities) {
        final Builder builder = ImmutableWriterConfig.builder().from(ExtractorTestUtil.config);
        if (commodityWhitelist != null) {
            builder.addAllReportingCommodityWhitelist(commodityWhitelist);
        }
        if (unaggregatedCommodities != null) {
            builder.unaggregatedCommodities(unaggregatedCommodities);
        }
        return builder.build();
    }

    /**
     * Test that the overall ingester flow in the {@link EntityMetricWriter} proceeds as expected
     * and results in the correct number of records reported as ingested.
     *
     * @throws InterruptedException        if interrupted
     * @throws SQLException                if there's a DB problem
     * @throws UnsupportedDialectException if the db endpoint is misconfigured
     * @throws IOException                 if there's an IO related issue
     */
    @Test
    public void testIngesterFlow() throws InterruptedException, SQLException, UnsupportedDialectException, IOException {
        final Consumer<TopologyEntityDTO> entityConsumer = writer.startTopology(
                info, ExtractorTestUtil.config, timer);
        final TopologyEntityDTO vm = mkEntity(VIRTUAL_MACHINE);
        entityConsumer.accept(vm);
        final TopologyEntityDTO pm = mkEntity(PHYSICAL_MACHINE);
        entityConsumer.accept(pm);
        int n = writer.finish(dataProvider);
        assertThat(n, is(2));
        // We didn't have any buys or sells in our entities
        assertThat(metricInsertCapture, is(empty()));
        // We had two entities total
        assertThat(entitiesUpsertCapture.size(), is(2));
        final List<Long> upsertedIds = entitiesUpsertCapture.stream().map(r -> r.get(ENTITY_OID_AS_OID))
                .collect(Collectors.toList());
        assertThat(upsertedIds, containsInAnyOrder(vm.getOid(), pm.getOid()));
        // We only had one topology, so no need to do any last-seen updates
        assertThat(entitiesUpdateCapture, is(empty()));
    }

    /**
     * Check that bought commodities result in correct metric records.
     *
     * @throws UnsupportedDialectException if endpoint is misconfigured
     * @throws InterruptedException        if interrupted
     * @throws SQLException                if there's a db problem
     * @throws IOException                 if an IO error
     */
    @Test
    public void testBoughtCommodityMetrics()
            throws UnsupportedDialectException, InterruptedException, SQLException, IOException {
        // set up for CPU and MEM, with the latter unaggregated when sold by a PM
        setupWriterAndSinks(ImmutableList.of(CPU_VALUE, MEM_VALUE),
                Collections.singletonMap(MEM, PHYSICAL_MACHINE));

        // process a vm buying CPU and MEM from a pm, each with multiple commodity keys
        final TopologyEntityDTO pm = mkEntity(PHYSICAL_MACHINE);
        final TopologyEntityDTO vm = mkEntity(VIRTUAL_MACHINE).toBuilder()
                .addCommoditiesBoughtFromProviders(boughtCommoditiesFromProvider(pm,
                        Triplet.with(CPU, "a", 1.0), Triplet.with(CPU, "b", 2.0),
                        Triplet.with(MEM, "a", 1.0), Triplet.with(MEM, "b", 2.0)
                )).build();
        int n = EntitiesProcessor.of(writer, info, config).process(vm).finish(dataProvider);
        // processed one entity
        assertThat(n, is(1));
        // we should have a single aggregated CPU metric, and two MEM metrics
        assertThat(metricInsertCapture.size(), is(3));
        Iterator<Record> records = metricInsertCapture.iterator();
        assertThat(records.next().asMap(), mapMatchesLaxly(
                createMetricRecordMap(null, vm.getOid(), null, MetricType.CPU, null, null, null, null, 3.0, pm.getOid()),
                TIME.getName(), ENTITY_HASH.getName()));
        assertThat(records.next().asMap(), mapMatchesLaxly(
                createMetricRecordMap(null, vm.getOid(), null, MetricType.MEM, "a", null, null, null, 1.0, pm.getOid()),
                TIME.getName(), ENTITY_HASH.getName()));
        assertThat(records.next().asMap(), mapMatchesLaxly(
                createMetricRecordMap(null, vm.getOid(), null, MetricType.MEM, "b", null, null, null, 2.0, pm.getOid()),
                TIME.getName(), ENTITY_HASH.getName()));
    }


    /**
     * Check that bought commodities result in correct metric records when values null.
     *
     * @throws UnsupportedDialectException if endpoint is misconfigured
     * @throws InterruptedException        if interrupted
     * @throws SQLException                if there's a db problem
     * @throws IOException                 if an IO error
     */
    @Test
    public void testBoughtCommodityMetricsWithNoValues()
                    throws UnsupportedDialectException, InterruptedException, SQLException, IOException {
        // set up for CPU and MEM, with the latter unaggregated when sold by a PM
        setupWriterAndSinks(ImmutableList.of(CPU_VALUE, MEM_VALUE),
                            Collections.singletonMap(MEM, PHYSICAL_MACHINE));

        // process a vm buying CPU and MEM from a pm, each with multiple commodity keys
        final TopologyEntityDTO pm = mkEntity(PHYSICAL_MACHINE);
        final TopologyEntityDTO vm = mkEntity(VIRTUAL_MACHINE).toBuilder()
                        .addCommoditiesBoughtFromProviders(boughtCommoditiesFromProvider(pm,
                                 Triplet.with(CPU, "a", null), Triplet.with(CPU, "b", null),
                                 Triplet.with(MEM, "a", null), Triplet.with(MEM, "b", null)
                        )).build();
        int n = EntitiesProcessor.of(writer, info, config).process(vm).finish(dataProvider);
        // processed one entity
        assertThat(n, is(1));
        // we should have a single aggregated CPU metric, and two MEM metrics
        assertThat(metricInsertCapture.size(), is(3));
        Iterator<Record> records = metricInsertCapture.iterator();
        assertThat(records.next().asMap(), mapMatchesLaxly(
                        createMetricRecordMap(null, vm.getOid(), null, MetricType.CPU, null, null, null, null, null, pm.getOid()),
                        TIME.getName(), ENTITY_HASH.getName()));
        assertThat(records.next().asMap(), mapMatchesLaxly(
                        createMetricRecordMap(null, vm.getOid(), null, MetricType.MEM, "a", null, null, null, null, pm.getOid()),
                        TIME.getName(), ENTITY_HASH.getName()));
        assertThat(records.next().asMap(), mapMatchesLaxly(
                        createMetricRecordMap(null, vm.getOid(), null, MetricType.MEM, "b", null, null, null, null, pm.getOid()),
                        TIME.getName(), ENTITY_HASH.getName()));
    }

    /**
     * Check that sold commodities result in correct metric records.
     *
     * @throws UnsupportedDialectException if endpoint is misconfigured
     * @throws InterruptedException        if interrupted
     * @throws SQLException                if there's a db problem
     * @throws IOException                 if an IO error
     */
    @Test
    public void testSoldCommodityMetrics()
            throws UnsupportedDialectException, InterruptedException, SQLException, IOException {
        // set up for CPU and MEM, with the latter unaggregated when sold by a PM
        setupWriterAndSinks(ImmutableList.of(CPU_VALUE, MEM_VALUE),
                Collections.singletonMap(MEM, PHYSICAL_MACHINE));

        // process a pm selling CPU and MEM, each with multiple commodity keys
        final TopologyEntityDTO pm = mkEntity(PHYSICAL_MACHINE).toBuilder()
                .addAllCommoditySoldList(soldCommodities(
                        Quartet.with(CPU, "a", 1.0, 10.0), Quartet.with(CPU, "b", 2.0, 20.0),
                        Quartet.with(MEM, "a", 1.0, 10.0), Quartet.with(MEM, "b", 2.0, 20.0)
                )).build();
        int n = EntitiesProcessor.of(writer, info, config).process(pm).finish(dataProvider);
        // processed one entity
        assertThat(n, is(1));
        // we should have a single aggregated CPU metric, and two MEM metrics
        assertThat(metricInsertCapture.size(), is(3));
        Iterator<Record> records = metricInsertCapture.iterator();
        assertThat(records.next().asMap(), mapMatchesLaxly(
                createMetricRecordMap(null, pm.getOid(), null, MetricType.CPU, null, 3.0, 30.0, 0.1, null, null),
                TIME.getName(), ENTITY_HASH.getName()));
        assertThat(records.next().asMap(), mapMatchesLaxly(
                createMetricRecordMap(null, pm.getOid(), null, MetricType.MEM, "a", 1.0, 10.0, 0.1, null, null),
                TIME.getName(), ENTITY_HASH.getName()));
        assertThat(records.next().asMap(), mapMatchesLaxly(
                createMetricRecordMap(null, pm.getOid(), null, MetricType.MEM, "b", 2.0, 20.0, 0.1, null, null),
                TIME.getName(), ENTITY_HASH.getName()));
    }

    /**
     * Check that sold commodities result in correct metric records when values not present.
     *
     * @throws UnsupportedDialectException if endpoint is misconfigured
     * @throws InterruptedException        if interrupted
     * @throws SQLException                if there's a db problem
     * @throws IOException                 if an IO error
     */
    @Test
    public void testSoldCommodityMetricsWithNoValues()
                    throws UnsupportedDialectException, InterruptedException, SQLException, IOException {
        // set up for CPU and MEM, with the latter unaggregated when sold by a PM
        setupWriterAndSinks(ImmutableList.of(CPU_VALUE, MEM_VALUE),
                            Collections.singletonMap(MEM, PHYSICAL_MACHINE));

        // process a pm selling CPU and MEM, each with multiple commodity keys
        final TopologyEntityDTO pm = mkEntity(PHYSICAL_MACHINE).toBuilder()
                        .addAllCommoditySoldList(soldCommodities(
                                        Quartet.with(CPU, "a", null, null), Quartet.with(CPU, "b", null, null),
                                        Quartet.with(MEM, "a", null, null), Quartet.with(MEM, "b", null, null)
                        )).build();
        int n = EntitiesProcessor.of(writer, info, config).process(pm).finish(dataProvider);
        // processed one entity
        assertThat(n, is(1));
        // we should have a single aggregated CPU metric, and two MEM metrics
        assertThat(metricInsertCapture.size(), is(3));
        Iterator<Record> records = metricInsertCapture.iterator();
        assertThat(records.next().asMap(), mapMatchesLaxly(
                        createMetricRecordMap(null, pm.getOid(), null, MetricType.CPU, null, null, null, null, null, null),
                        TIME.getName(), ENTITY_HASH.getName()));
        assertThat(records.next().asMap(), mapMatchesLaxly(
                        createMetricRecordMap(null, pm.getOid(), null, MetricType.MEM, "a", null, null, null, null, null),
                        TIME.getName(), ENTITY_HASH.getName()));
        assertThat(records.next().asMap(), mapMatchesLaxly(
                        createMetricRecordMap(null, pm.getOid(), null, MetricType.MEM, "b", null, null, null, null, null),
                        TIME.getName(), ENTITY_HASH.getName()));
    }

    /**
     * Check that aggregated sold commodities result in correct metric records when not all present .
     *
     * @throws UnsupportedDialectException if endpoint is misconfigured
     * @throws InterruptedException        if interrupted
     * @throws SQLException                if there's a db problem
     * @throws IOException                 if an IO error
     */
    @Test
    public void testSoldCommodityMetricsAggregationWithSomeValuesMissing()
                    throws UnsupportedDialectException, InterruptedException, SQLException, IOException {
        // set up for CPU and MEM, with the latter unaggregated when sold by a PM
        setupWriterAndSinks(ImmutableList.of(CPU_VALUE, MEM_VALUE),
                            Collections.singletonMap(MEM, PHYSICAL_MACHINE));

        // process a pm selling CPU and MEM, each with multiple commodity keys
        final TopologyEntityDTO pm = mkEntity(PHYSICAL_MACHINE).toBuilder()
                        .addAllCommoditySoldList(soldCommodities(
                                        Quartet.with(CPU, "a", 2.0, 4.0), Quartet.with(CPU, "b", null, null),
                                        Quartet.with(MEM, "a", 3.0, null), Quartet.with(MEM, "b", null, null),
                                        Quartet.with(MEM, "c", null, 3.0)
                        )).build();
        int n = EntitiesProcessor.of(writer, info, config).process(pm).finish(dataProvider);
        // processed one entity
        assertThat(n, is(1));
        // we should have a single aggregated CPU metric, and two MEM metrics
        assertThat(metricInsertCapture.size(), is(4));
        Iterator<Record> records = metricInsertCapture.iterator();
        assertThat(records.next().asMap(), mapMatchesLaxly(
                        createMetricRecordMap(null, pm.getOid(), null, MetricType.CPU, null, 2.0, 4.0, 0.5, null, null),
                        TIME.getName(), ENTITY_HASH.getName()));
        assertThat(records.next().asMap(), mapMatchesLaxly(
                        createMetricRecordMap(null, pm.getOid(), null, MetricType.MEM, "a", 3.0, null, null, null, null),
                        TIME.getName(), ENTITY_HASH.getName()));
        assertThat(records.next().asMap(), mapMatchesLaxly(
                        createMetricRecordMap(null, pm.getOid(), null, MetricType.MEM, "b", null, null, null, null, null),
                        TIME.getName(), ENTITY_HASH.getName()));
        assertThat(records.next().asMap(), mapMatchesLaxly(
                        createMetricRecordMap(null, pm.getOid(), null, MetricType.MEM, "c", null, 3.0, null, null, null),
                        TIME.getName(), ENTITY_HASH.getName()));
    }

    /**
     * Test that all VMs' ready queue commodities are renamed to same commodity name, and converted
     * to sold commodity with capacity and utilization.
     *
     * @throws SQLException                if there's a DB problem
     * @throws UnsupportedDialectException if the db endpoint is misconfigured
     * @throws IOException                 if there's an IO related issue
     * @throws InterruptedException        if interrupted
     */
    @Test
    public void testQxVCPUMetric() throws InterruptedException, SQLException, UnsupportedDialectException, IOException {
        // create a PM and some VMs buying from it
        final TopologyEntityDTO pm = mkEntity(PHYSICAL_MACHINE).toBuilder()
                .addAllCommoditySoldList(soldCommodities(
                        Quartet.with(CommodityDTO.CommodityType.Q64_VCPU, null, 0.0, 20000.0)))
                .build();
        final Map<Long, TopologyEntityDTO> vmsById = Stream.of(
                CommodityType.Q1_VCPU, CommodityType.Q2_VCPU, CommodityType.Q3_VCPU,
                CommodityType.Q4_VCPU, CommodityType.Q5_VCPU, CommodityType.Q6_VCPU,
                CommodityType.Q7_VCPU, CommodityType.Q8_VCPU, CommodityType.Q16_VCPU,
                CommodityType.Q32_VCPU, CommodityType.Q64_VCPU, CommodityType.QN_VCPU)
                .map(commodityType -> mkEntity(VIRTUAL_MACHINE).toBuilder()
                        .addCommoditiesBoughtFromProviders(boughtCommoditiesFromProvider(pm,
                                Triplet.with(commodityType, null, 50.0)))
                        .build())
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        // process our entities
        int n = EntitiesProcessor.of(writer, info, config)
                .process(pm)
                .process(vmsById.values())
                .finish(dataProvider);
        // we processed 1 pm and all the vms, and each produced a single metric record
        assertThat(n, is(vmsById.size() + 1));
        assertThat(metricInsertCapture.size(), is(n));
        final Map<Long, Record> records = metricInsertCapture.stream()
                .collect(Collectors.toMap(r -> r.get(ENTITY_OID), r -> r));
        records.forEach((oid, record) -> {
            if (oid == pm.getOid()) {
                // verify that pm's Q64_VCPU is not changed
                assertThat(record.get(COMMODITY_TYPE), is(MetricType.valueOf(CommodityType.forNumber(
                        pm.getCommoditySoldList(0).getCommodityType().getType()).name())));
            } else {
                final TopologyEntityDTO vm = vmsById.get(oid);
                final double boughtUsed =
                        vm.getCommoditiesBoughtFromProviders(0).getCommodityBought(0).getUsed();
                // verify that vm's Qx_VCPU is renamed, and changed to sold commodity
                assertThat(record.get(COMMODITY_TYPE), is(VM_QX_VCPU_NAME));
                assertThat(record.get(COMMODITY_CURRENT), is(boughtUsed));
                assertThat(record.get(COMMODITY_CAPACITY),
                        is(TopologyDTOUtil.QX_VCPU_BASE_COEFFICIENT));
                assertThat(record.get(COMMODITY_UTILIZATION),
                        is(boughtUsed / TopologyDTOUtil.QX_VCPU_BASE_COEFFICIENT));
                assertThat(record.get(COMMODITY_CONSUMED), is(nullValue()));
                assertThat(record.get(COMMODITY_PROVIDER), is(nullValue()));
            }
        });
    }

    /**
     * Test that wasted files are ingested correctly for on-prem case. For onprem, only wasted
     * files on volume2 are persisted, since volume1 is used by vm1 and volume3 is on storage2
     * whose wasted files should be ignored. For cloud, wasted files on volume4 should also be
     * persisted.
     *         vm1
     *          |
     *          |
     *       volume1        volume2          cloud volume3
     *    (used files)   (wasted files)     (wasted files)
     *          \         /                       |
     *           \       /                        |
     *           storage1                    storageTier1
     *
     * @throws InterruptedException        if interrupted
     * @throws SQLException                if there's a DB problem
     * @throws UnsupportedDialectException if the db endpoint is misconfigured
     * @throws IOException                 if there's an IO related issue
     */
    @Test
    public void testWastedFilesIngestion()
            throws UnsupportedDialectException, SQLException, IOException, InterruptedException {
        final Consumer<TopologyEntityDTO> entityConsumer = writer.startTopology(
                info, ExtractorTestUtil.config, timer);

        final TopologyEntityDTO storage1 = mkEntity(STORAGE);

        final List<VirtualVolumeFileDescriptor> filesList1 = Arrays.asList(
                file("/var/vmware-0.log", LOG, 202, 0),
                file("/test/small-flat.vmdk", DISK, 2609152, 0),
                file("/as-kube-node-3/as-kube-node-3-3b62dc2c.vswp", SWAP, 16777216, 0));
        final TopologyEntityDTO volume1 = onPremVolume(filesList1, AttachmentState.ATTACHED, storage1.getOid());

        final List<VirtualVolumeFileDescriptor> filesList2 = Arrays.asList(
                file("/var/a.log", LOG, 200, 1581941078000L),
                file("/foo/diags.zip", CONFIGURATION, 21065, 1580146549000L),
                file("/bar/hyperv.iso", ISO, 8866, 1580146546000L));
        final TopologyEntityDTO volume2 = onPremVolume(filesList2, AttachmentState.UNATTACHED, storage1.getOid());

        final TopologyEntityDTO vm = mkEntity(VIRTUAL_MACHINE).toBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(volume1.getOid())
                        .setConnectedEntityType(VIRTUAL_VOLUME.getNumber()))
                .build();

        final TopologyEntityDTO storageTier1 = mkEntity(STORAGE_TIER);
        final List<VirtualVolumeFileDescriptor> filesList3 = Arrays.asList(
                file("/disks/wasted", DISK, 109152, 1581941078000L),
                file("/foo/diags.zip", CONFIGURATION, 21065, 1580146549000L));
        final TopologyEntityDTO volume3 = cloudVolume(filesList3, AttachmentState.UNATTACHED, storageTier1.getOid());

        // mock
        doReturn(Optional.of(storage1.getDisplayName())).when(dataProvider).getDisplayName(storage1.getOid());
        doReturn(Optional.of(storageTier1.getDisplayName())).when(dataProvider).getDisplayName(storageTier1.getOid());

        // write
        List<TopologyEntityDTO> entities = Arrays.asList(vm, volume1, volume2, volume3, storage1,
                storageTier1);
        // shuffle so the order of receiving entity is randomized each time
        Collections.shuffle(entities);
        entities.forEach(entityConsumer);
        writer.finish(dataProvider);

        // verify that the files on volume2 and volume4 are persisted
        assertThat(wastedFileReplacerCapture.size(), is(5));

        final Map<Long, TopologyEntityDTO> storageById = Stream.of(storage1, storageTier1)
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        final Map<Long, Map<String, VirtualVolumeFileDescriptor>> wastedFileByStorageAndPath = ImmutableMap.of(
                storage1.getOid(), filesList2.stream()
                        .collect(Collectors.toMap(VirtualVolumeFileDescriptor::getPath, Function.identity())),
                storageTier1.getOid(), filesList3.stream()
                        .collect(Collectors.toMap(VirtualVolumeFileDescriptor::getPath, Function.identity()))
        );

        for (Record record : wastedFileReplacerCapture) {
            final Long storageId = record.get(STORAGE_OID);
            final VirtualVolumeFileDescriptor expected =
                    wastedFileByStorageAndPath.get(storageId).get(record.get(FILE_PATH));
            assertThat(record.get(FILE_SIZE), is(expected.getSizeKb()));
            assertThat(record.get(MODIFICATION_TIME).getTime(), is(expected.getModificationTimeMs()));
            assertThat(record.get(STORAGE_NAME), is(storageById.get(storageId).getDisplayName()));
        }
    }
}
