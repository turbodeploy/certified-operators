package com.vmturbo.history.stats.live;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.jooq.DSLContext;
import org.jooq.InsertSetMoreStep;
import org.jooq.Query;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkInserter;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.IngestersConfig;
import com.vmturbo.history.schema.abstraction.tables.records.EntitiesRecord;
import com.vmturbo.history.utils.SystemLoadHelper;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Test persisting Topology DTO's to the DB.
 */
public class EntityStatsWriterTest {
    // TODO unify: revive

    private static final long TEST_OID = 123;

    private static final int writeTopologyChunkSize = 100;

    private static final TopologyInfo TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setTopologyContextId(12341)
            .setTopologyId(11111)
            .build();

    private static final EntityDTO.EntityType sdkEntityType =
            EntityDTO.EntityType.VIRTUAL_DATACENTER;
    private static final int vmEntityTypeNumber = sdkEntityType.getNumber();
    private static final EntityType dbEntityType = EntityType.VIRTUAL_MACHINE;
    private static final EntityType otherDbEntityType = EntityType.PHYSICAL_MACHINE;
    private static final String displayName = "displayName";
    private static final ImmutableList<String> commodityExcludeList = ImmutableList.of(
            "ApplicationCommodity", "CLUSTERCommodity", "DATACENTERCommodity", "DATASTORECommodity",
            "DSPMAccessCommodity", "NETWORKCommodity");

    private EntitiesRecord mockEntitiesRecord;
    private HistorydbIO mockHistorydbIO;
    private GroupServiceBlockingStub groupServiceClient;
    private SystemLoadHelper systemLoadHelper;
    private DSLContext mockDSLContext;
    private Collection<TopologyEntityDTO> allEntities;
    @Captor
    private ArgumentCaptor<List<EntitiesRecord>> persistedEntititesCaptor;
    private SimpleBulkLoaderFactory mockLoaders;
    private BulkInserter mockWriter;
    private int dbExecutions = 0;

    private <T> Answer<T> dbCounter(T value) {
        return invocation -> {
            dbExecutions += 1;
            return value;
        };
    }

    /**
     * Create and configure mocks required for tests.
     *
     * @throws Exception if there's a problem
     */
    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);

        mockHistorydbIO = Mockito.mock(HistorydbIO.class);
        mockDSLContext = Mockito.mock(DSLContext.class);
        mockLoaders = Mockito.mock(SimpleBulkLoaderFactory.class);
        mockWriter = Mockito.mock(BulkInserter.class);

        // the entities to persist, and TopologyOrganizer, etc.
        allEntities = Lists.newArrayList(
                buildEntityDTO(sdkEntityType, TEST_OID, displayName));

        groupServiceClient = Mockito.mock(IngestersConfig.class).groupServiceBlockingStub();
        systemLoadHelper = Mockito.mock(SystemLoadHelper.class);

        // this is so we can check real insertion counts
        doCallRealMethod().when(mockWriter).insertAll(any());
        // make sure mocked writer gets closed (so we get dbcount) when writers gets closed
        // let all execute calls get to the three-arg method, so we can count them easily
        when(mockHistorydbIO.execute(any(), anyList())).then(dbCounter(Collections.emptyList()));
        when(mockHistorydbIO.execute(any(), any(Query.class))).then(dbCounter(Collections.emptyList()));
// TODO        when(mockHistorydbIO.execute(any(), any(), any())).then(dbCounter(null));
        // also, we'd get one execution per writer in a real setting (maybe more than one, but
        // we'll assume one)
        when(mockLoaders.getLoader(any())).then(invocation -> {
            dbExecutions++;
            return mockWriter;
        });

        // mock bind values for counting inserted rows
        InsertSetMoreStep mockInsertStep = Mockito.mock(InsertSetMoreStep.class);
        when(mockInsertStep.getBindValues()).thenReturn(Lists.newArrayList(new Object()));

        // mock the response to fetching
        mockEntitiesRecord = Mockito.mock(EntitiesRecord.class);
// TODO        when(mockHistorydbIO.getCommodityInsertStatement(any())).thenReturn(mockInsertStep);

        // mock entity type lookup utilities
        when(mockHistorydbIO.getEntityType(vmEntityTypeNumber)).thenReturn(
                Optional.of(dbEntityType));
        when(mockHistorydbIO.getBaseEntityType(vmEntityTypeNumber)).thenReturn(
                Optional.of(dbEntityType.getClsName()));
    }

    private void consumeDTOs() throws Exception {
//        LiveStatsWriter testStatsWriter = new LiveStatsWriter(
//            mockHistorydbIO,
//            commodityExcludeList,
//            RecordWriterUtils.getRecordWriterConfig(),
//            RecordWriterUtils.getRecordWritersThreadPool());
        RemoteIterator<TopologyEntityDTO> allDTOs = Mockito.mock(RemoteIterator.class);
        when(allDTOs.hasNext()).thenReturn(true).thenReturn(false);
        when(allDTOs.nextChunk()).thenReturn(allEntities);

//        testStatsWriter.processChunks(TOPOLOGY_INFO, allDTOs, groupServiceClient, systemLoadHelper);
    }

    /**
     * Test the case where the entity already exists, and entity type matches.
     * In this case, there should only one DLSContext.execute()" called, to query the Entity.
     * There should be no "DSLContext.execute(insert())" called.
     *
     * @throws Exception if there's a problem
     */
    @Ignore // TODO
    @Test
    public void testWhenEntityExists() throws Exception {
        // Arrange

        // entity found
        setupEntitiesTableQuery(displayName, dbEntityType.getClsName());

        // Act
        consumeDTOs();

        // Assert
        verifyEntityWasNotUpdated();

    }

    /**
     * Test the case where the entity already exists, and entity type matches but
     * displayName changes.
     * In this case, there should two DLSContext.execute()" calls, one to query the Entity and one
     * to insert the entity information.
     *
     * @throws Exception if there's a problem
     */
    @Ignore // TODO
    @Test
    public void testWhenDisplayNameChanges() throws Exception {
        // Arrange

        // entity found
        String otherDisplayName = "otherDisplayName";
        setupEntitiesTableQuery(otherDisplayName, dbEntityType.getClsName());

        // Act
        consumeDTOs();

        // Assert
        verifyEntityWasUpdated();

    }

    private void verifyEntityWasUpdated() throws VmtDbException, InterruptedException {
        // check types for known entities
        verify(mockHistorydbIO, times(1)).getEntities(any());
        verify(mockHistorydbIO, times(2)).getEntityType(sdkEntityType.getNumber());
        // records written for entity and metrics
        verify(mockWriter, times(2)).insert(any());
        // three DB executions - two by records writers, and one by projected stat record
        assertEquals("DB executions", 3, dbExecutions);

        verifyNoMoreInteractions(mockDSLContext);
    }

    /**
     * Test the case where the entity already exists, and entity type changes but
     * displayName remains the same.
     * In this case, there should two DLSContext.execute()" calls, one to query the Entity and one
     * to insert the entity information.
     *
     * @throws Exception if there's a problem
     */
    @Ignore // TODO
    @Test
    public void testWhenEntityTypeChanges() throws Exception {
        // Arrange
        setupEntitiesTableQuery(displayName, otherDbEntityType.getClsName());

        // Act
        consumeDTOs();

        // Assert
        verifyEntityWasUpdated();
    }

    /**
     * Test the case where the entity does not already exist. In this case, there should
     * two DLSContext.execute()" calls, one to query the Entity and one to insert the
     * entity information.
     *
     * @throws Exception if there's a problem
     */
    @Ignore // TODO
    @Test
    public void testWhenEntityDoesNotExist() throws Exception {

        // Arrange
        setupEntitiesTableQuery(null, null);

        // Act
        consumeDTOs();

        // check types for known entities
        verify(mockHistorydbIO, times(1)).getEntities(any());
        verify(mockHistorydbIO, times(2)).getEntityType(sdkEntityType.getNumber());
        // record written for entity and for metrics

        verify(mockWriter, times(2)).insert(any());
        // 3 DB executions - two by writers (one would be empty), and one for projected stat record
        assertEquals("DB executions", 3, dbExecutions);

        verifyNoMoreInteractions(mockDSLContext);
    }

    /**
     * Common setup for historydbIO.getAllEntities() - to fetch the known entities.
     *
     * @param displayName the displayName to return; or null indicating no entries in the result
     * @param clsName     class name to return
     * @throws VmtDbException if there's a problem
     */
    private void setupEntitiesTableQuery(String displayName, String clsName) throws VmtDbException {

        ImmutableMap.Builder<Long, EntitiesRecord> allEntitiesMapBuilder =
                new ImmutableMap.Builder<>();
        if (displayName != null) {
            when(mockEntitiesRecord.getDisplayName()).thenReturn(displayName);
            when(mockEntitiesRecord.getCreationClass()).thenReturn(clsName);
            allEntitiesMapBuilder.put(TEST_OID, mockEntitiesRecord);
        }
        Map<Long, EntitiesRecord> allEntitiesMap = allEntitiesMapBuilder.build();
        when(mockHistorydbIO.getEntities(any())).thenReturn(allEntitiesMap);
    }

    private void verifyEntityWasNotUpdated() throws VmtDbException, InterruptedException {
        // check types for known entities
        verify(mockHistorydbIO, times(1)).getEntities(any());
        verify(mockHistorydbIO, times(2)).getEntityType(sdkEntityType.getNumber());
        // record written only for metrics
        verify(mockWriter, times(1)).insert(any());
        // 3 DB executions - two by writers (one would be empty), and one for projected stat record
        assertEquals("DB executions", 3, dbExecutions);

        verifyNoMoreInteractions(mockDSLContext);
    }

    /**
     * Create a {@link TopologyEntityDTO} with the given entity type and oid.
     *
     * @param sdkEntityType the {@link EntityDTO.EntityType} type for the new
     *                      {@link TopologyEntityDTO}
     * @param newOID        the unique ID for the new {@link TopologyEntityDTO}
     * @param displayName   the human-facing name from dicovery
     * @return a new {@link TopologyEntityDTO} with the given entity type and oid
     */
    private TopologyEntityDTO buildEntityDTO(EntityDTO.EntityType sdkEntityType, long newOID,
            String displayName) {
        return TopologyEntityDTO.newBuilder()
                .setOid(newOID)
                .setEntityType(sdkEntityType.getNumber())
                .setDisplayName(displayName)
                .build();
    }
}
