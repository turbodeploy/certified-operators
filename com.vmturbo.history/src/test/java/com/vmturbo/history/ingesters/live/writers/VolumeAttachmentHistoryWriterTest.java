package com.vmturbo.history.ingesters.live.writers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Date;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.history.VolAttachmentHistoryOuterClass;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.history.db.bulk.BulkLoaderMock;
import com.vmturbo.history.db.bulk.DbMock;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor.ChunkDisposition;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase.IngesterState;
import com.vmturbo.history.ingesters.live.writers.VolumeAttachmentHistoryWriter.Factory;
import com.vmturbo.history.notifications.VolAttachmentDaysSender;
import com.vmturbo.history.schema.abstraction.tables.VolumeAttachmentHistory;
import com.vmturbo.history.schema.abstraction.tables.records.VolumeAttachmentHistoryRecord;
import com.vmturbo.history.stats.readers.VolumeAttachmentHistoryReader;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for VolumeAttachmentHistoryWriter.
 */
public class VolumeAttachmentHistoryWriterTest {

    private static final long CREATION_TIME = 1604527665698L;
    private static final TopologyInfo TOPOLOGY_INFO = TopologyInfo.newBuilder()
        .setCreationTime(CREATION_TIME)
        .setTopologyType(TopologyType.REALTIME)
        .build();
    private static final String INFO_SUMMARY = TopologyDTOUtil
        .getSourceTopologyLabel(TOPOLOGY_INFO);
    private static final long VOLUME_1_OID = 1000;
    private static final long VOLUME_2_OID = 1001;
    private static final long VOLUME_3_OID = 1002;
    private static final long VM_1_OID = 2000;
    private static final long VM_2_OID = 2001;

    private VolumeAttachmentHistoryWriter writer;
    private DbMock dbMock;
    private SimpleBulkLoaderFactory loaderFactory;
    private IngesterState ingesterState;

    /**
     * Initialize test resources.
     */
    @Before
    public void setup() {
        dbMock = new DbMock();
        loaderFactory = new BulkLoaderMock(dbMock).getFactory();
        ingesterState = new IngesterState(loaderFactory, null, null);
        writer = (VolumeAttachmentHistoryWriter)new VolumeAttachmentHistoryWriter.Factory(1,
                mock(VolAttachmentDaysSender.class))
            .getChunkProcessor(TOPOLOGY_INFO, ingesterState).orElse(null);
        Assert.assertNotNull(writer);
    }

    /**
     * Test that empty chunk returns SUCCESS.
     *
     * @throws InterruptedException if processEntities is interrupted.
     */
    @Test
    public void testEmptyChunk() throws InterruptedException {
        Assert.assertEquals(ChunkDisposition.SUCCESS,
            writer.processEntities(Collections.emptyList(),
            INFO_SUMMARY));
        Assert.assertTrue(dbMock.getRecords(VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY)
            .isEmpty());
    }

    /**
     * Test that no records are inserted for non-cloud entities.
     *
     * @throws InterruptedException if processEntities is interrupted.
     */
    @Test
    public void testNoInsertForNonCloudEntity() throws InterruptedException {
        // given
        final TopologyEntityDTO entity = createVmEntityDto(EnvironmentType.ON_PREM, VM_1_OID,
            VOLUME_1_OID);
        // when
        writer.processEntities(Collections.singleton(entity), INFO_SUMMARY);
        // then
        Assert.assertTrue(dbMock.getRecords(VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY)
            .isEmpty());
    }

    /**
     * Test that a single record is inserted while processing a Cloud VM attached with a single
     * Volume.
     *
     * @throws InterruptedException if processEntities is interrupted.
     */
    @Test
    public void testProcessEntitiesVmConsumeFromSingleVolume() throws InterruptedException {
        // given
        final TopologyEntityDTO entity = createVmEntityDto(EnvironmentType.CLOUD, VM_1_OID,
            VOLUME_1_OID);
        // when
        writer.processEntities(Collections.singleton(entity), INFO_SUMMARY);
        // then
        verifyRecords(createExpectedRecords(VM_1_OID, VOLUME_1_OID),
            new HashSet<>(dbMock.getRecords(VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY)));
    }

    /**
     * Test that 2 records are inserted while processing a Cloud VM attached to 2 Volumes.
     *
     * @throws InterruptedException if processEntities is interrupted.
     */
    @Test
    public void testProcessEntitiesVmConsumesFromTwoVolumes() throws InterruptedException {
        // given
        final TopologyEntityDTO entity = createVmEntityDto(EnvironmentType.CLOUD, VM_1_OID,
            VOLUME_1_OID, VOLUME_2_OID);
        // when
        writer.processEntities(Collections.singleton(entity), INFO_SUMMARY);
        // then
        verifyRecords(createExpectedRecords(VM_1_OID, VOLUME_1_OID, VOLUME_2_OID),
            new HashSet<>(dbMock.getRecords(VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY)));
    }

    /**
     * Test that records related to each VM are inserted while processing a chunk containing
     * multiple Cloud VMs.
     *
     * @throws InterruptedException if processEntities is interrupted.
     */
    @Test
    public void testProcessEntitiesMultipleVMChunk() throws InterruptedException {
        // given
        final TopologyEntityDTO entity1 = createVmEntityDto(EnvironmentType.CLOUD, VM_1_OID,
            VOLUME_1_OID, VOLUME_2_OID);
        final TopologyEntityDTO entity2 = createVmEntityDto(EnvironmentType.CLOUD, VM_2_OID,
            VOLUME_3_OID);
        // when
        writer.processEntities(Stream.of(entity1, entity2).collect(Collectors.toSet()),
            INFO_SUMMARY);
        // then
        final Set<VolumeAttachmentHistoryRecord> expectedRecordsEntity1 =
            createExpectedRecords(VM_1_OID, VOLUME_1_OID, VOLUME_2_OID);
        final Set<VolumeAttachmentHistoryRecord> expectedRecordsEntity2 =
            createExpectedRecords(VM_2_OID, VOLUME_3_OID);
        verifyRecords(Stream.of(expectedRecordsEntity1, expectedRecordsEntity2)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet()),
            new HashSet<>(dbMock.getRecords(VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY)));
    }

    /**
     * Test that 2 records are inserted when a single Volume is attached to 2 VMs.
     *
     * @throws InterruptedException if processEntities is interrupted.
     */
    @Test
    public void testProcessEntitiesMultiAttachVolume() throws InterruptedException {
        // given
        final TopologyEntityDTO entity1 = createVmEntityDto(EnvironmentType.CLOUD, VM_1_OID,
            VOLUME_1_OID);
        final TopologyEntityDTO entity2 = createVmEntityDto(EnvironmentType.CLOUD, VM_2_OID,
            VOLUME_1_OID);
        // when
        writer.processEntities(Stream.of(entity1, entity2).collect(Collectors.toSet()),
            INFO_SUMMARY);
        // then
        final Set<VolumeAttachmentHistoryRecord> expectedRecordsEntity1 =
            createExpectedRecords(VM_1_OID, VOLUME_1_OID);
        final Set<VolumeAttachmentHistoryRecord> expectedRecordsEntity2 =
            createExpectedRecords(VM_2_OID, VOLUME_1_OID);
        final Set<VolumeAttachmentHistoryRecord> actualRecords =
            new HashSet<>(dbMock.getRecords(VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY));
        verifyRecords(Stream.of(expectedRecordsEntity1, expectedRecordsEntity2)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet()), actualRecords);
        Assert.assertEquals(2, actualRecords.size());
    }

    /**
     * Test that record related to unattached volume is inserted with the placeholder VM OID.
     *
     * @throws InterruptedException if processEntities is interrupted.
     */
    @Test
    public void testUnattachedVolume() throws InterruptedException {
        final TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
            .setOid(VOLUME_1_OID)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .build();
        writer.processEntities(Collections.singleton(entity), INFO_SUMMARY);
        writer.finish(0, false, INFO_SUMMARY);
        final long placeholderVmOid = 0;
        verifyRecords(createExpectedRecords(placeholderVmOid, VOLUME_1_OID),
            new HashSet<>(dbMock.getRecords(VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY)));
    }

    /**
     * Test that notification related to unattached volume is interacted.
     *
     * @throws InterruptedException if sendMessage is interrupted.
     */
    @Test
    public void testUnattachedVolumeNotification() throws InterruptedException, CommunicationException {

        IMessageSender<VolAttachmentHistoryOuterClass.VolAttachmentHistory> messageSender = mock(IMessageSender.class);
        VolumeAttachmentHistoryReader reader =  mock(VolumeAttachmentHistoryReader.class);
        ExecutorService executorService = mock(ExecutorService.class);
        VolAttachmentDaysSender sender = new VolAttachmentDaysSender(messageSender, reader, executorService);
        doNothing().when(messageSender).sendMessage(any());
        ArgumentCaptor<List> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
        when(reader.getVolumeAttachmentHistory(listArgumentCaptor.capture())).thenReturn(anyList());
        sender.readAndSendVolumeNotification(Collections.singletonList(VOLUME_1_OID), 2L);

        verify(messageSender).sendMessage(any());
        assertEquals(Collections.singletonList(VOLUME_1_OID), listArgumentCaptor.getValue());
    }

    /**
     * Test that Factory returns an empty Optional if the difference between last insert topology
     * creation time and current topology creation time is less than the defined interval between
     * inserts.
     */
    @Test
    public void testFactoryGetChunkProcessorWithinInsertInterval() {
        final long intervalBetweenInserts = 1;
        final VolumeAttachmentHistoryWriter.Factory factory = new Factory(intervalBetweenInserts,
                mock(VolAttachmentDaysSender.class));
        // first topology always results in Writer being returned
        Assert.assertTrue(factory.getChunkProcessor(TOPOLOGY_INFO, ingesterState).isPresent());
        // next topology after ten minutes, with interval defined as 1 hour should return empty
        // optional
        final TopologyInfo nextBroadcast = TOPOLOGY_INFO.toBuilder()
            .setCreationTime(CREATION_TIME + TimeUnit.MINUTES.toMillis(10))
            .build();
        Assert.assertFalse(factory.getChunkProcessor(nextBroadcast, ingesterState).isPresent());
    }

    /**
     * Test that Factory returns the Writer if the difference between last insert topology
     * creation time and current topology creation time is equal to the defined interval between
     * inserts.
     */
    @Test
    public void testFactoryGetChunkProcessorOutsideInsertInterval() {
        final long intervalBetweenInserts = 1;

        final VolumeAttachmentHistoryWriter.Factory factory = new Factory(intervalBetweenInserts,
                mock(VolAttachmentDaysSender.class));
        // first topology always results in Writer being returned
        Assert.assertTrue(factory.getChunkProcessor(TOPOLOGY_INFO, ingesterState).isPresent());
        // next topology after 1 hour, with interval defined as 1 hour result should not return
        // empty optional
        final TopologyInfo nextBroadcast = TOPOLOGY_INFO.toBuilder()
            .setCreationTime(CREATION_TIME + TimeUnit.HOURS.toMillis(1))
            .build();
        Assert.assertTrue(factory.getChunkProcessor(nextBroadcast, ingesterState).isPresent());
    }

    private void verifyRecords(final Set<VolumeAttachmentHistoryRecord> expectedRecords,
                               final Set<VolumeAttachmentHistoryRecord> actualRecords) {
        Assert.assertEquals(expectedRecords, actualRecords);
    }

    private TopologyEntityDTO createVmEntityDto(final EnvironmentType environmentType,
                                                final long vmOid,
                                                final long... volumeOid) {
        final Set<CommoditiesBoughtFromProvider> commoditiesBoughtFromProviders =
            Arrays.stream(volumeOid).boxed()
                .map(volOid -> CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(volOid)
                    .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                    .build())
                .collect(Collectors.toSet());
        return TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEnvironmentType(environmentType)
            .setOid(vmOid)
            .addAllCommoditiesBoughtFromProviders(commoditiesBoughtFromProviders)
            .build();
    }

    private Set<VolumeAttachmentHistoryRecord> createExpectedRecords(final long vmOid,
                                                                     final long... volumeOid) {
        final Date topologyDate = new Date(TOPOLOGY_INFO.getCreationTime());
        return Arrays.stream(volumeOid).boxed()
            .map(volOid -> new VolumeAttachmentHistoryRecord(volOid, vmOid, topologyDate,
                topologyDate))
            .collect(Collectors.toSet());
    }
}
