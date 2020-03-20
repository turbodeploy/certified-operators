package com.vmturbo.history.ingesters.live.writers;

import static com.vmturbo.common.protobuf.utils.StringConstants.COMPUTE_TIER;
import static com.vmturbo.common.protobuf.utils.StringConstants.NETWORK;
import static com.vmturbo.history.schema.abstraction.tables.Entities.ENTITIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Functions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.EntityType.UseCase;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkLoaderMock;
import com.vmturbo.history.db.bulk.DbMock;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.schema.abstraction.tables.records.EntitiesRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Test that {@link EntitiesWriter} creates/updates records correctly in database.
 */
public class EntitiesWriterTest {

    private static final String TOPOLOGY_SUMMARY = "test topology";
    private static final EntityType COMPUTE_TIER_ENTITY_TYPE = EntityType.get(COMPUTE_TIER);
    private static final EntityType NETWORK_ENTITY_TYPE = EntityType.get(NETWORK);
    private static final EntityType VM_ENTITY_TYPE = EntityType.get(StringConstants.VIRTUAL_MACHINE);

    private HistorydbIO historydbIO;
    private TopologyInfo topologyInfo;
    private DbMock dbMock = new DbMock();
    private SimpleBulkLoaderFactory loaders = new BulkLoaderMock(dbMock).getFactory();
    private IChunkProcessor<Topology.DataSegment> writer;

    /**
     * Set up for testing.
     *
     * @throws VmtDbException if db operation fails (shouldn't happen with our mocks)
     */
    @Before
    public void before() throws VmtDbException {
        historydbIO = mock(HistorydbIO.class);
        when(historydbIO.getEntityType(anyInt())).thenCallRealMethod();
        // make HistorydbIO#getEntities(oids) lookup the given entity oids in ourx dbMock
        doAnswer((Answer<Map<Long, EntitiesRecord>>)invocation -> {
            List arg0 = invocation.getArgumentAt(0, List.class);
            @SuppressWarnings("unchecked")
            final List<String> oidStrings = (List<String>)arg0;
            final Object[] oidsForVarargs = oidStrings.stream().map(Long::valueOf).toArray();
            return dbMock.getRecords(ENTITIES, oidsForVarargs).stream()
                    .collect(Collectors.toMap(EntitiesRecord::getId, Functions.identity()));
        }).when(historydbIO).getEntities(anyListOf(String.class));

        topologyInfo = TopologyInfo.newBuilder()
                .setCreationTime(System.currentTimeMillis())
                .build();
        writer = new EntitiesWriter.Factory(historydbIO)
                .getChunkProcessor(topologyInfo, loaders)
                // null can't happen in this test, but just using get() causes compiler warning
                .orElse(null);
    }

    /**
     * Test that new entities appearing in a topology are recorded in the database.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testNewEntitiesRecorded() throws InterruptedException {
        List<List<Topology.DataSegment>> chunks = createChunks(100, 10, 3);
        for (List<Topology.DataSegment> chunk : chunks) {
            writer.processChunk(chunk, TOPOLOGY_SUMMARY);
        }
        writer.finish(10, false, "test topology");
        assertEquals(10, dbMock.getRecords(ENTITIES).size());
    }

    /**
     * Test that entities that were previously recorded are not inserted when they appear
     * again unchnanged in a later topology.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testKnownEntitiesPreserved() throws InterruptedException {
        List<List<Topology.DataSegment>> firstBroadcast = createChunks(100, 2, 2);
        writer.processChunk(firstBroadcast.get(0), TOPOLOGY_SUMMARY);
        writer.finish(2, false, "test topology");
        final Set<Integer> initialRecordIds = dbMock.getRecords(ENTITIES).stream()
                .map(System::identityHashCode)
                .collect(Collectors.toSet());
        writer = new EntitiesWriter.Factory(historydbIO)
                .getChunkProcessor(topologyInfo, loaders).orElse(null);
        List<List<Topology.DataSegment>> secondBroadcast = createChunks(100, 10, 3);
        for (List<Topology.DataSegment> chunk : secondBroadcast) {
            writer.processChunk(chunk, TOPOLOGY_SUMMARY);
        }
        writer.finish(10, false, "test topology");
        final Set<Integer> allRecordIds = dbMock.getRecords(ENTITIES).stream()
                .map(System::identityHashCode)
                .collect(Collectors.toSet());
        assertTrue(allRecordIds.containsAll(initialRecordIds));
    }

    /**
     * Test that entities that were previously recorded are updated in the database if they
     * are changed in a later topology.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testUpdatedEntitiesUpdated() throws InterruptedException {
        // initial topology has oids 100 and 101
        List<List<Topology.DataSegment>> firstBroadcast = createChunks(100, 2, 2);
        writer.processChunk(firstBroadcast.get(0), TOPOLOGY_SUMMARY);
        writer.finish(2, false, "test topology");
        // get object ids for records for oids 100 and 101
        final int orig100 = System.identityHashCode(dbMock.getRecord(ENTITIES, 100L));
        final int orig101 = System.identityHashCode(dbMock.getRecord(ENTITIES, 101L));

        // second has oids 100 through 109; 100 is changed 101 is not
        writer = new EntitiesWriter.Factory(historydbIO)
                .getChunkProcessor(topologyInfo, loaders).orElse(null);
        List<List<Topology.DataSegment>> secondBroadcast = createChunks(100, 10, 3);
        // make a change to entity100
        final TopologyEntityDTO altered100 = TopologyEntityDTO.newBuilder()
                .mergeFrom(secondBroadcast.get(0).get(0).getEntity())
                .setDisplayName("altered")
                .build();
        secondBroadcast.get(0).set(0,
                Topology.DataSegment.newBuilder().setEntity(altered100).build());
        for (List<Topology.DataSegment> chunk : secondBroadcast) {
            writer.processChunk(chunk, TOPOLOGY_SUMMARY);
        }
        writer.finish(10, false, "test topology");
        // make sure we have the same records for both 100 and 100L (EntitiesWriter reuses existing
        // record object when it decides to update)
        assertEquals(orig100, System.identityHashCode(dbMock.getRecord(ENTITIES, 100L)));
        assertEquals(orig101, System.identityHashCode(dbMock.getRecord(ENTITIES, 101L)));
        // but we have the altered display name in 100
        assertEquals("altered", dbMock.getRecord(ENTITIES, 100L).getDisplayName());
    }

    /**
     * Test that if entities appear in the topology with associated {@link EntityType} values that
     * do not carry the {@link UseCase#PersistEntity} use-case, those entities are not
     * persisted.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testThatEntityTypesWithoutPersistEntitiesUseCaseAreSkipped() throws InterruptedException {
        writer.processChunk(makeSingletonChunk(COMPUTE_TIER_ENTITY_TYPE, 1L, "CTier1"), "test topology");
        writer.processChunk(makeSingletonChunk(NETWORK_ENTITY_TYPE, 2L, "Net1"), "test topology");
        loaders.flushAll();
        final Collection<EntitiesRecord> entitiesWritten = dbMock.getRecords(ENTITIES);
        assertEquals(1, entitiesWritten.size());
        assertEquals(COMPUTE_TIER_ENTITY_TYPE.getName(),
                dbMock.getRecord(ENTITIES, 1L).get(ENTITIES.CREATION_CLASS));
        assertNull(dbMock.getRecord(ENTITIES, 2L));
    }

    /**
     * Ensure that entities with display names that exceed the schema limit are truncated to the
     * allowed size.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testThatLongDisplayNamesAreTruncated() throws InterruptedException {
        final String maxName = Strings.repeat("x", EntitiesWriter.ENTITY_DISPLAY_NAME_MAX_LENGTH);
        final String tooLongName = Strings.repeat("y", EntitiesWriter.ENTITY_DISPLAY_NAME_MAX_LENGTH + 1);
        writer.processChunk(makeSingletonChunk(VM_ENTITY_TYPE, 1L, maxName), "test topology");
        writer.processChunk(makeSingletonChunk(VM_ENTITY_TYPE, 2L, tooLongName), "test topology");
        loaders.flushAll();
        final Collection<EntitiesRecord> entitiesWritten = dbMock.getRecords(ENTITIES);
        assertEquals(2, entitiesWritten.size());
        assertEquals(maxName, dbMock.getRecord(ENTITIES, 1L).getDisplayName());
        assertEquals(tooLongName.substring(0, EntitiesWriter.ENTITY_DISPLAY_NAME_MAX_LENGTH),
                dbMock.getRecord(ENTITIES, 2L).getDisplayName());
    }

    private List<List<Topology.DataSegment>> createChunks(
            final int startId, final int n, int chunkSize) {
        List<List<Topology.DataSegment>> chunks = new ArrayList<>();
        List<Topology.DataSegment> currentChunk = null;
        for (int i = 0; i < n; i++) {
            if (i % chunkSize == 0) {
                currentChunk = new ArrayList<>();
                chunks.add(currentChunk);
            }
            final TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
                    .setOid(startId + i)
                    .setDisplayName("entity #" + (startId + i))
                    .setEntityType(EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                    .build();
            currentChunk.add(Topology.DataSegment.newBuilder().setEntity(entity).build());
        }
        return chunks;
    }

    private Collection<DataSegment> makeSingletonChunk(
            final EntityType entityType, final long oid, final String name) {
        final TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType.getSdkEntityType().get().getNumber())
                .setDisplayName(name)
                .build();
        return ImmutableList.of(DataSegment.newBuilder().setEntity(entity).build());
    }

}
