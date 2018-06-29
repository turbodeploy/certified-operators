package com.vmturbo.history.stats.live;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jooq.DSLContext;
import org.jooq.InsertSetMoreStep;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.records.EntitiesRecord;
import com.vmturbo.history.stats.live.LiveStatsWriter;
import com.vmturbo.history.topology.TopologySnapshotRegistry;
import com.vmturbo.history.utils.TopologyOrganizer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Test persisting Topology DTO's to the DB.
 */
public class LiveStatsWriterTest {

    private static final long TEST_OID = 123;
    private static int writeTopologyChunkSize = 100;
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
    private TopologySnapshotRegistry topologySnapshotRegistry;
    private TopologyOrganizer topologyOrganizer;
    private DSLContext mockDSLContext;
    private Collection<TopologyEntityDTO> allEntities;
    @Captor
    private ArgumentCaptor<List<EntitiesRecord>> persistedEntitiesCaptor;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);

        mockHistorydbIO = Mockito.mock(HistorydbIO.class);
        mockDSLContext = Mockito.mock(DSLContext.class);

        // the entities to persist, and TopologyOrganizer, etc.
        allEntities = Lists.newArrayList(
                buildEntityDTO(sdkEntityType, TEST_OID, displayName));
        topologySnapshotRegistry = Mockito.mock(TopologySnapshotRegistry.class);
        topologyOrganizer = Mockito.mock(TopologyOrganizer.class);

        // mock bind values for counting inserted rows
        InsertSetMoreStep mockInsertStep = Mockito.mock(InsertSetMoreStep.class);
        when(mockInsertStep.getBindValues()).thenReturn(Lists.newArrayList(new Object()));

        // mock the response to fetching
        mockEntitiesRecord = Mockito.mock(EntitiesRecord.class);
        when(mockHistorydbIO.getCommodityInsertStatement(any())).thenReturn(mockInsertStep);

        // mock entity type lookup utilities
        when(mockHistorydbIO.getEntityType(vmEntityTypeNumber)).thenReturn(
                Optional.of(dbEntityType));
        when(mockHistorydbIO.getBaseEntityType(vmEntityTypeNumber)).thenReturn(
                Optional.of(dbEntityType.getClsName()));

    }

    private void consumeDTOs() throws Exception {
        LiveStatsWriter testStatsWriter = new LiveStatsWriter(topologySnapshotRegistry,
                mockHistorydbIO, writeTopologyChunkSize, commodityExcludeList);
        RemoteIterator<TopologyEntityDTO> allDTOs = Mockito.mock(RemoteIterator.class);
        when(allDTOs.hasNext()).thenReturn(true).thenReturn(false);
        when(allDTOs.nextChunk()).thenReturn(allEntities);

        testStatsWriter.processChunks(topologyOrganizer, allDTOs);
    }

    /**
     * Test the case where the entity already exists, and entity type matches.
     * In this case, there should only one DLSContext.execute()" called, to query the Entity.
     * There should be no "DSLContext.execute(insert())" called.
     */
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
     */
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

    private void verifyEntityWasUpdated() throws VmtDbException {
        // check types for known entities
        verify(mockHistorydbIO, times(1)).getEntities(any());
        // don't persist the entity
        verify(mockHistorydbIO, times(1)).persistEntities(persistedEntitiesCaptor.capture());
        Assert.assertEquals(1, persistedEntitiesCaptor.getValue().size());
        // one test for entity type
        verify(mockHistorydbIO, times(2)).getEntityType(sdkEntityType.getNumber());
        // write aggregate stats and market stats in one chunk
        verify(mockHistorydbIO, times(1)).execute(any(HistorydbIO.Style.class),
                any(List.class));
        verifyNoMoreInteractions(mockDSLContext);
    }

    /**
     * Test the case where the entity already exists, and entity type changes but
     * displayName remains the same.
     * In this case, there should two DLSContext.execute()" calls, one to query the Entity and one
     * to insert the entity information.
     */
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
     */
    @Test
    public void testWhenEntityDoesNotExist() throws Exception {

        // Arrange
        setupEntitiesTableQuery(null, null);

        // Act
        consumeDTOs();

        // Assert
        // check types for known entities
        verify(mockHistorydbIO, times(1)).getEntities(any());
        // persist the entity
        verify(mockHistorydbIO, times(1)).persistEntities(persistedEntitiesCaptor.capture());
        Assert.assertEquals(1, persistedEntitiesCaptor.getValue().size());
        // look up to calculate _latest table
        verify(mockHistorydbIO, times(2)).getEntityType(sdkEntityType.getNumber());
        // write aggregate stats and market stats
        verify(mockHistorydbIO, times(1)).execute(any(HistorydbIO.Style.class), any(List.class));
        verifyNoMoreInteractions(mockDSLContext);
    }

    /**
     * Common setup for historydbIO.getAllEntities() - to fetch the known entities.
     *
     * @param displayName the displayName to return; or null indicating no entries in the result
     * @param clsName class name to return
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

    private void verifyEntityWasNotUpdated() throws VmtDbException {
        // check types for all known the entities
        verify(mockHistorydbIO, times(1)).getEntities(any());
        // don't persist the entity
        verify(mockHistorydbIO, times(1)).persistEntities(persistedEntitiesCaptor.capture());
        Assert.assertEquals(0, persistedEntitiesCaptor.getValue().size());
        // one test for entity type
        verify(mockHistorydbIO, times(2)).getEntityType(sdkEntityType.getNumber());
        // One execute() to insert the aggregate stats and market stats (chunked)
        verify(mockHistorydbIO, times(1)).execute(any(HistorydbIO.Style.class),
                any(List.class));
        verifyNoMoreInteractions(mockDSLContext);
    }
    /**
     * Create a {@link TopologyEntityDTO} with the given entity type and oid.
     *
     * @param sdkEntityType the {@link EntityDTO.EntityType} type for the new
     * {@link TopologyEntityDTO}
     * @param newOID the unique ID for the new {@link TopologyEntityDTO}
     * @param displayName the human-facing name from dicovery
     * @return a new {@link TopologyEntityDTO} with the given entity type and oid
     */
    private TopologyEntityDTO buildEntityDTO(EntityDTO.EntityType sdkEntityType,long newOID,
                                             String displayName) {
        return TopologyEntityDTO.newBuilder()
                .setOid(newOID)
                .setEntityType(sdkEntityType.getNumber())
                .setDisplayName(displayName)
                .build();
    }
}