package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static com.vmturbo.extractor.util.TopologyTestUtil.mkEntity;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.APPLICATION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_ACCOUNT;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_USER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.COMPUTE_TIER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DATABASE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DESKTOP_POOL;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DISK_ARRAY;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.LOGICAL_POOL;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.REGION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE_CONTROLLER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.MessageOrBuilder;

import org.hamcrest.Matcher;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslUpdateRecordSink;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.ExtractorDbConfig;
import com.vmturbo.extractor.util.ExtractorTestUtil;
import com.vmturbo.extractor.util.TopologyTestUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Tests of EntityMetricWriter.
 */
public class EntityMetricWriterTest {
    DbEndpoint endpoint = spy(new ExtractorDbConfig().ingesterEndpoint());
    EntityMetricWriter writer = spy(new EntityMetricWriter(endpoint,
            new EntityHashManager(ExtractorTestUtil.config),
            Executors.newSingleThreadScheduledExecutor()));
    final TopologyInfo info = TopologyTestUtil.mkRealtimeTopologyInfo(1L);
    final MultiStageTimer timer = mock(MultiStageTimer.class);
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
     */
    @Before
    public void before() throws UnsupportedDialectException, SQLException {
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        DslRecordSink entitiesUpserterSink = mock(DslUpsertRecordSink.class);
        this.entitiesUpsertCapture = captureSink(entitiesUpserterSink, false);
        DslRecordSink entitiesUpdaterSink = mock(DslUpdateRecordSink.class);
        this.entitiesUpdateCapture = captureSink(entitiesUpdaterSink, false);
        DslRecordSink metricInsertrSink = mock(DslRecordSink.class);
        this.metricInsertCapture = captureSink(metricInsertrSink, false);
        doReturn(entitiesUpserterSink).when(writer).getEntityUpsertSink(
                any(DSLContext.class), any(), any());
        doReturn(entitiesUpdaterSink).when(writer).getEntityUpdaterSink(
                any(DSLContext.class), any(), any(), any());
        doReturn(metricInsertrSink).when(writer).getMetricInserterSink(any(DSLContext.class));
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
                            String.format("Unknwon field %s for entity type %s",
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
     * Test that the overall ingester flow in the {@link EntityMetricWriter} proceeds as expected
     * and results in the correct number of records reported as ingested.
     *
     * @throws InterruptedException        if interrupted
     * @throws SQLException if there's a DB problem
     * @throws UnsupportedDialectException if the db endpint is misconfigured
     * @throws IOException if there's an IO related issue
     */
    @Test
    public void testIngesterFlow() throws InterruptedException, SQLException, UnsupportedDialectException, IOException {
        final Consumer<TopologyEntityDTO> entityConsumer = writer.startTopology(
                info, Collections.emptyMap(), ExtractorTestUtil.config, timer);
        final TopologyEntityDTO vm = mkEntity(VIRTUAL_MACHINE);
        entityConsumer.accept(vm);
        final TopologyEntityDTO pm = mkEntity(PHYSICAL_MACHINE);
        entityConsumer.accept(pm);
        int n = writer.finish(Collections.emptyMap());
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
}
