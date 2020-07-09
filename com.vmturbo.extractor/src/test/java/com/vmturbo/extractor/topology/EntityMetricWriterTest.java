package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CAPACITY;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CONSUMED;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CURRENT;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_PROVIDER;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_TYPE;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_UTILIZATION;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.topology.EntityMetricWriter.VM_QX_VCPU_NAME;
import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static com.vmturbo.extractor.util.TopologyTestUtil.boughtCommodityFromProvider;
import static com.vmturbo.extractor.util.TopologyTestUtil.mkEntity;
import static com.vmturbo.extractor.util.TopologyTestUtil.soldCommodity;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.APPLICATION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_ACCOUNT;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_USER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.COMPUTE_TIER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DATABASE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DATABASE_SERVER_TIER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DATABASE_TIER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DESKTOP_POOL;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DISK_ARRAY;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.LOGICAL_POOL;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.REGION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE_CONTROLLER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.WORKLOAD_CONTROLLER;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.MessageOrBuilder;

import org.hamcrest.Matcher;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.TypeCase;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslUpdateRecordSink;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.util.ExtractorTestUtil;
import com.vmturbo.extractor.util.TopologyTestUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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
        final DbEndpoint endpoint = spy(dbConfig.ingesterEndpoint());
        this.writer = spy(new EntityMetricWriter(endpoint,
                new EntityHashManager(ExtractorTestUtil.config),
                Executors.newSingleThreadScheduledExecutor()));
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        DslRecordSink entitiesUpserterSink = mock(DslUpsertRecordSink.class);
        this.entitiesUpsertCapture = captureSink(entitiesUpserterSink, false);
        DslRecordSink entitiesUpdaterSink = mock(DslUpdateRecordSink.class);
        this.entitiesUpdateCapture = captureSink(entitiesUpdaterSink, false);
        DslRecordSink metricInserterSink = mock(DslRecordSink.class);
        this.metricInsertCapture = captureSink(metricInserterSink, false);
        doReturn(entitiesUpserterSink).when(writer).getEntityUpsertSink(
                any(DSLContext.class), any(), any());
        doReturn(entitiesUpdaterSink).when(writer).getEntityUpdaterSink(
                any(DSLContext.class), any(), any(), any());
        doReturn(metricInserterSink).when(writer).getMetricInserterSink(any(DSLContext.class));
        doReturn(Stream.empty()).when(dataProvider).getAllGroups();
    }

    /**
     * Test that all types of entities appearing in a topology retain only intended fields in their
     * type-specific info, when being persisted (to the "attrs" column of the entity table) in the
     * database.
     */
    @Test
    public void testTypeSpecificInfoStripping() {
        checkStripping(APPLICATION, "ip_address");
        checkStripping(BUSINESS_ACCOUNT, "account_id", "associated_target_id",
                null, "riSupported");
        checkExcludedType(BUSINESS_USER);
        checkStripping(COMPUTE_TIER, "family", "num_coupons", "num_cores",
                null, "burstableCPU", "dedicated_storage_network_state", "instance_disk_type",
                "instance_disk_size_gb", "num_instance_disks", "quota_family",
                "supported_customer_info");
        checkStripping(DATABASE, "edition", "engine", "license_model", "deployment_type", "version");
        checkStripping(DESKTOP_POOL, "assignment_type", "provision_type", "clone_type",
                "template_reference_id",
                null, "vm_with_snapshot");
        checkStripping(DISK_ARRAY, "disk_type_info");
        checkStripping(LOGICAL_POOL, "disk_type_info");
        checkStripping(PHYSICAL_MACHINE, "cpu_model", "vendor", "model", "num_cpus", "timezone",
                "num_cpu_sockets", "cpu_core_mhz", "diskGroup");
        checkExcludedType(REGION);
        checkStripping(STORAGE, "external_name", "storage_type", "is_local",
                null, "ignore_wasted_files", "policy", "rawCapacity");
        checkStripping(STORAGE_CONTROLLER, "disk_type_info");
        checkStripping(VIRTUAL_MACHINE, "guest_os_info", "ip_addresses", "num_cpus",
                "connected_networks", "license_model", "dynamic_memory",
                null, "driverInfo", "locks", "tenancy", "architecture", "billingType",
                "virtualizationType");
        checkStripping(VIRTUAL_VOLUME,
                null, "files");
        checkExcludedType(WORKLOAD_CONTROLLER);
        checkExcludedType(DATABASE_TIER);
        checkExcludedType(DATABASE_SERVER_TIER);
    }

    private void checkStripping(
            EntityType entityType, String... fields) {
        TopologyEntityDTO entity = mkEntity(entityType);
        final MessageOrBuilder stripped =
                EntityMetricWriter.stripUnwantedFields(entity.getTypeSpecificInfo());
        Matcher<Boolean> matcher = is(true);
        Function<String, String> labeler = field -> String.format(
                "Entity of type %s was missing expected field %s", entityType, field);
        for (final String field : fields) {
            if (field == null) {
                matcher = is(false);
                labeler = getter -> String.format(
                        "Entity of type %s has unexpected field %s", entityType, getter);
            } else {
                FieldDescriptor desc = stripped.getDescriptorForType().findFieldByName(field);
                if (desc != null) {
                    boolean present = desc.isRepeated()
                            ? stripped.getRepeatedFieldCount(desc) > 0
                            : stripped.hasField(desc);
                    assertThat(labeler.apply(field), present, matcher);
                } else {
                    throw new IllegalArgumentException(
                            String.format("Unknown field %s for entity type %s",
                                    field, entityType));
                }
            }
        }
    }

    private void checkExcludedType(EntityType entityType) {
        TopologyEntityDTO entity = mkEntity(entityType);
        final MessageOrBuilder stripped =
                EntityMetricWriter.stripUnwantedFields(entity.getTypeSpecificInfo());
        assertThat(stripped, is(nullValue()));
    }

    /**
     * Make sure that the type-specific-info attributes stripper in {@link EntityMetricWriter} covers
     * all entity types for which there is a {@link TypeSpecificInfo} type defined in
     * {@link TopologyEntityDTO}.
     */
    @Test
    public void testAllTypeSpecificInfosTested() {
        for (final TypeCase type : TypeCase.values()) {
            if (type != TypeCase.TYPE_NOT_SET) {
                EntityType entityType = EntityType.valueOf(type.name());
                final TopologyEntityDTO entity = mkEntity(entityType);
                assertThat(entity.getTypeSpecificInfo().getTypeCase(), is(type));
                try {
                    EntityMetricWriter.stripUnwantedFields(entity.getTypeSpecificInfo());
                } catch (Exception e) {
                    fail(String.format("Type-specific-info type %s not supported by field-stripper", type));
                }
            }
        }
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
     * Test that all VMs' ready queue commodities are renamed to same commodity name, and converted
     * to sold commodity with capacity and utilization.
     *
     * @throws InterruptedException        if interrupted
     * @throws SQLException                if there's a DB problem
     * @throws UnsupportedDialectException if the db endpint is misconfigured
     * @throws IOException                 if there's an IO related issue
     */
    @Test
    public void testQxVCPUMetric() throws InterruptedException, SQLException, UnsupportedDialectException, IOException {
        final Consumer<TopologyEntityDTO> entityConsumer = writer.startTopology(
                info, ExtractorTestUtil.config, timer);
        final TopologyEntityDTO pm = mkEntity(PHYSICAL_MACHINE).toBuilder()
                .addCommoditySoldList(soldCommodity(CommodityDTO.CommodityType.Q64_VCPU, 0, 20000))
                .build();

        final Map<Long, TopologyEntityDTO> vmsById = Stream.of(
                CommodityType.Q1_VCPU, CommodityType.Q2_VCPU, CommodityType.Q3_VCPU,
                CommodityType.Q4_VCPU, CommodityType.Q5_VCPU, CommodityType.Q6_VCPU,
                CommodityType.Q7_VCPU, CommodityType.Q8_VCPU, CommodityType.Q16_VCPU,
                CommodityType.Q32_VCPU, CommodityType.Q64_VCPU, CommodityType.QN_VCPU)
                .map(commodityType -> mkEntity(VIRTUAL_MACHINE).toBuilder()
                        .addCommoditiesBoughtFromProviders(
                                boughtCommodityFromProvider(commodityType, 50, pm.getOid()))
                        .build())
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        vmsById.values().forEach(entityConsumer::accept);
        entityConsumer.accept(pm);
        // write
        writer.finish(dataProvider);
        // check there are two records
        assertThat(metricInsertCapture.size(), is(vmsById.size() + 1));
        final Map<Long, Record> records = metricInsertCapture.stream()
                .collect(Collectors.toMap(r -> r.get(ENTITY_OID), r -> r));
        records.forEach((oid, record) -> {
            if (oid == pm.getOid()) {
                // verify that pm's Q64_VCPU is not changed
                assertThat(record.get(COMMODITY_TYPE), is(CommodityType.forNumber(
                        pm.getCommoditySoldList(0).getCommodityType().getType()).name()));
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
}
