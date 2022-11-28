package com.vmturbo.history.ingesters.live.writers;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationServiceInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.history.db.bulk.BulkLoaderMock;
import com.vmturbo.history.db.bulk.DbMock;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor.ChunkDisposition;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase.IngesterState;
import com.vmturbo.history.ingesters.live.writers.ApplicationServiceDaysEmptyWriter.Factory;
import com.vmturbo.history.notifications.ApplicationServiceHistoryNotificationSender;
import com.vmturbo.history.schema.abstraction.tables.ApplicationServiceDaysEmpty;
import com.vmturbo.history.schema.abstraction.tables.records.ApplicationServiceDaysEmptyRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for ApplicationServiceDaysEmptyWriter.
 */
public class ApplicationServiceDaysEmptyWriterTest {

    private static final ApplicationServiceDaysEmpty ASDE = ApplicationServiceDaysEmpty.APPLICATION_SERVICE_DAYS_EMPTY;

    private static final TopologyInfo TOPOLOGY_INFO = createNewTopologyInfo();

    private ApplicationServiceDaysEmptyWriter writer;
    private DbMock dbMock;
    private SimpleBulkLoaderFactory loaderFactory;
    private IngesterState ingesterState;
    private DSLContext dslContext = mock(DSLContext.class);

    /**
     * Initialize test resources.
     */
    @Before
    public void setup() {
        setupDbMock();
        loaderFactory = new BulkLoaderMock(dbMock).getFactory();
        ingesterState = new IngesterState(loaderFactory, null, null);
        writer = new TestApplicationServiceDaysEmptyWriter(
                TOPOLOGY_INFO,
                ingesterState.getLoaders(),
                mock(ApplicationServiceHistoryNotificationSender.class),
                dslContext);
        Assert.assertNotNull(writer);
    }

    private void setupDbMock() {
        dbMock = new DbMock();
        dbMock.setTableKeys(ASDE, Arrays.asList(ASDE.ID));
    }

    private static String getTopologyLabel() {
        return TopologyDTOUtil.getSourceTopologyLabel(TOPOLOGY_INFO);
    }

    private static TopologyInfo getTopologyInfo() {
        return TOPOLOGY_INFO;
    }

    static TopologyInfo createNewTopologyInfo() {
        return createNewTopologyInfo(System.currentTimeMillis());
    }

    static TopologyInfo createNewTopologyInfo(long creationTime) {
        return TopologyInfo.newBuilder()
                .setCreationTime(creationTime)
                .setTopologyType(TopologyType.REALTIME)
                .build();
    }

    private static long getTopologyCreationTime() {
        return getTopologyInfo().getCreationTime();
    }

    /**
     * Test that empty chunk returns SUCCESS.
     *
     * @throws InterruptedException if processEntities is interrupted.
     */
    @Test
    public void testEmptyChunk() throws InterruptedException {
        ChunkDisposition chunkDisposition = writer.processEntities(Collections.emptyList(),
                getTopologyLabel());
        assertEquals(ChunkDisposition.SUCCESS, chunkDisposition);
        assertTrue(dbMock.getRecords(ASDE).isEmpty());
    }

    /**
     * Test that entities are stored correctly.
     * VirtualMachineSpec entities with apps are not stored.
     * VirtualMachineSpec entities without apps are stored.
     * Non VirtualMachineSpec entities are ignored.
     *
     * @throws InterruptedException if processEntities is interrupted.
     */
    @Test
    public void testProcessEntities() throws InterruptedException {
        // given
        final TopologyEntityDTO wrongType = createVmEntityDto(1L, "wrongType");
        final TopologyEntityDTO withApp1 = createVmSpecEntityDto(2L, "withApp1", 1);
        final TopologyEntityDTO withApp2 = createVmSpecEntityDto(3L, "withApp2", 2);
        final TopologyEntityDTO withApp3 = createVmSpecEntityDto(4L, "withApp3", 3);
        final TopologyEntityDTO noApp1 = createVmSpecEntityDto(5L, "noApp1", 0);
        final TopologyEntityDTO noApp2 = createVmSpecEntityDto(6L, "noApp2", 0);
        List<TopologyEntityDTO> entities = Arrays.asList(
                wrongType,
                withApp1,
                withApp2,
                withApp3,
                noApp1,
                noApp2
        );
        // when
        processEntities(writer, entities);
        // then
        List<ApplicationServiceDaysEmptyRecord> expected = createExpectedRecords(noApp1, noApp2);
        Collection<ApplicationServiceDaysEmptyRecord> actual = dbMock.getRecords(ASDE);
        assertThat(actual.size(), equalTo(2));
        assertThat(actual, containsInAnyOrder(expected.toArray(new ApplicationServiceDaysEmptyRecord[0])));
    }

    /**
     * Test that Factory returns an empty chunk processor if the difference between last insert
     * topology creation time and current topology creation time is less than the defined interval
     * between inserts.
     */
    @Test
    public void testFactoryGetChunkProcessorWithinInsertInterval() {
        final long ten_minutes = 10;
        final Factory factory = new Factory(ten_minutes,
                mock(ApplicationServiceHistoryNotificationSender.class),
                dslContext);
        // first topology always results in Writer being returned
        assertTrue(factory.getChunkProcessor(TOPOLOGY_INFO, ingesterState).isPresent());
        // next topology within update threshold should not return the writer
        TopologyInfo nextBroadcast = getTopologyInfo().toBuilder()
                .setCreationTime(getTopologyCreationTime() + TimeUnit.MINUTES.toMillis(ten_minutes - 1))
                .build();
        assertFalse(factory.getChunkProcessor(nextBroadcast, ingesterState).isPresent());
        // next topology after update threshold should not return the writer
        nextBroadcast = getTopologyInfo().toBuilder()
                .setCreationTime(getTopologyCreationTime() + TimeUnit.MINUTES.toMillis(ten_minutes + 1))
                .build();
        assertTrue(factory.getChunkProcessor(nextBroadcast, ingesterState).isPresent());
    }

    static void processEntities(ApplicationServiceDaysEmptyWriter writer, List<TopologyEntityDTO> entities)
            throws InterruptedException {
        ChunkDisposition chunkDisposition = writer.processEntities(entities, getTopologyLabel());
        assertEquals(ChunkDisposition.SUCCESS, chunkDisposition);
        writer.finish(entities.size(), false, getTopologyLabel());
    }

    static TopologyEntityDTO createVmSpecEntityDto(final long oid, String name, int appCount) {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_SPEC_VALUE)
                .setOid(oid)
                .setDisplayName(name)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setApplicationService(ApplicationServiceInfo.newBuilder()
                                .setAppCount(appCount)
                                .build()
                        ).build())
                .build();
    }

    private TopologyEntityDTO createVmEntityDto(final long oid, String name) {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(oid)
                .setDisplayName(name)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();
    }

    private List<ApplicationServiceDaysEmptyRecord> createExpectedRecords(TopologyEntityDTO... entities) {
        Timestamp tpCreationTime = new Timestamp(getTopologyCreationTime());
        return Arrays.stream(entities)
                .map(e -> new ApplicationServiceDaysEmptyRecord(e.getOid(),
                        e.getDisplayName(),
                        tpCreationTime,
                        tpCreationTime))
                .collect(Collectors.toList());
    }

    /**
     * A test subclass of ApplicationServiceDaysEmptyWriter that enables us to override the
     * deleteAppServicesWithApps method with a no-op.
     */
    private class TestApplicationServiceDaysEmptyWriter extends ApplicationServiceDaysEmptyWriter {

        protected TestApplicationServiceDaysEmptyWriter(@NotNull TopologyInfo topologyInfo,
                @NotNull SimpleBulkLoaderFactory loaderFactory,
                @NotNull ApplicationServiceHistoryNotificationSender appSvcHistorySender,
                @NotNull DSLContext dslContext) {
            super(topologyInfo, loaderFactory, appSvcHistorySender, dslContext);
        }

        @Override
        protected int deleteAppServicesWithApps() {
            // We do this instead of trying to mock DslContext.
            return 0;
        }
    }
}
