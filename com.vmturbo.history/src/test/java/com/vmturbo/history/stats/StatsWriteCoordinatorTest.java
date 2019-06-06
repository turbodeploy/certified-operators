package com.vmturbo.history.stats;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.writers.AbstractStatsWriter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Checks that {@link StatsWriteCoordinator} is working as expected.
 */
public class StatsWriteCoordinatorTest {
    private static final VmtDbException EXPECTED_CHUNK_EXCEPTION =
                    new VmtDbException(VmtDbException.SQL_EXEC_ERR,
                                    "Unexpected error while topology chunk processing");
    private static final VmtDbException EXPECTED_COMPLETE_EXCEPTION =
                    new VmtDbException(VmtDbException.SQL_EXEC_ERR,
                                    "Unexpected error while complete topology processing");
    private static final int MAX_WRITE_TOPOLOGY_CHUNK_SIZE = 10;
    private ExecutorService statsWritersPool;
    private TopologyInfo topologyInfo;

    /**
     * Initializes all resources required for tests.
     */
    @Before
    public void before() {
        statsWritersPool = Executors.newCachedThreadPool();
        topologyInfo = TopologyInfo.newBuilder().build();
    }

    /**
     * Releases all resources occupied by tests.
     */
    @After
    public void after() {
        statsWritersPool.shutdownNow();
    }

    /**
     * Checks that in case {@link StatsWriteCoordinator} will be initialized without writers and there are no
     * entities for processing then nothing will be broken.
     *
     * @throws InterruptedException in case test thread will be interrupted
     * @throws TimeoutException in case data receiving process will not be completed
     *                 in desired amount of time.
     * @throws CommunicationException in case data receiving process will fail due
     *                 to connection issues.
     */
    @Test
    public void checkWithoutWritersAndEntities()
                    throws InterruptedException, TimeoutException, CommunicationException {
        final StatsWriteCoordinator statsWriteCoordinator = new StatsWriteCoordinator(statsWritersPool, Collections.emptySet(),
                        Collections.emptySet(), MAX_WRITE_TOPOLOGY_CHUNK_SIZE);
        statsWriteCoordinator.processChunks(topologyInfo, mockRemoteIterator());
    }

    /**
     * Checks that in case {@link StatsWriteCoordinator} will be initialized without writers then nothing will
     * be broken.
     *
     * @throws InterruptedException in case test thread will be interrupted
     * @throws TimeoutException in case data receiving process will not be completed
     *                 in desired amount of time.
     * @throws CommunicationException in case data receiving process will fail due
     *                 to connection issues.
     */
    @Test
    public void checkWithoutWriters()
                    throws InterruptedException, TimeoutException, CommunicationException {
        final StatsWriteCoordinator statsWriteCoordinator = new StatsWriteCoordinator(statsWritersPool, Collections.emptySet(),
                        Collections.emptySet(), MAX_WRITE_TOPOLOGY_CHUNK_SIZE);
        statsWriteCoordinator.processChunks(topologyInfo, mockRemoteIterator(
                        Collections.singleton(createEntity(EntityType.VIRTUAL_MACHINE, 1))));
    }

    /**
     * Checks that in case {@link StatsWriteCoordinator} {@link RemoteIterator} will provide empty collection
     * then nothing will be broken.
     *
     * @throws InterruptedException in case test thread will be interrupted
     * @throws TimeoutException in case data receiving process will not be completed
     *                 in desired amount of time.
     * @throws CommunicationException in case data receiving process will fail due
     *                 to connection issues.
     */
    @Test
    public void checkWithoutEntities()
                    throws InterruptedException, TimeoutException, CommunicationException {
        final StatsWriteCoordinator statsWriteCoordinator = new StatsWriteCoordinator(statsWritersPool, Collections.emptySet(),
                        Collections.singleton(new TestCompleteTopologyWriter(0)),
                        MAX_WRITE_TOPOLOGY_CHUNK_SIZE);
        statsWriteCoordinator.processChunks(topologyInfo, mockRemoteIterator());
    }

    /**
     * Checks that {@link StatsWriteCoordinator} will properly handle chunk from one entity using one complete
     * topology writer.
     *
     * @throws InterruptedException in case test thread will be interrupted
     * @throws TimeoutException in case data receiving process will not be completed
     *                 in desired amount of time.
     * @throws CommunicationException in case data receiving process will fail due
     *                 to connection issues.
     */
    @Test
    public void checkOneEntitiesChunk()
                    throws InterruptedException, TimeoutException, CommunicationException {
        final StatsWriteCoordinator statsWriteCoordinator = new StatsWriteCoordinator(statsWritersPool, Collections.emptySet(),
                        Collections.singleton(new TestCompleteTopologyWriter(1)),
                        MAX_WRITE_TOPOLOGY_CHUNK_SIZE);
        statsWriteCoordinator.processChunks(topologyInfo, mockRemoteIterator(
                        Collections.singleton(createEntity(EntityType.VIRTUAL_MACHINE, 1))));
    }

    /**
     * Checks that in case {@link StatsWriteCoordinator} process the chunk of entities which is bigger then
     * configured maximmum number of entities to be written in one chunk then it will automatically
     * will split this chunk in subchunks.
     *
     * @throws InterruptedException in case test thread will be interrupted
     * @throws TimeoutException in case data receiving process will not be completed
     *                 in desired amount of time.
     * @throws CommunicationException in case data receiving process will fail due
     *                 to connection issues.
     */
    @Test
    public void checkOneBigEntitiesChunks()
                    throws InterruptedException, TimeoutException, CommunicationException {
        final StatsWriteCoordinator statsWriteCoordinator = new StatsWriteCoordinator(statsWritersPool, Collections.singleton(
                        new TestChunkedTopologyWriter(5, MAX_WRITE_TOPOLOGY_CHUNK_SIZE)),
                        Collections.singleton(new TestCompleteTopologyWriter(15)),
                        MAX_WRITE_TOPOLOGY_CHUNK_SIZE);
        statsWriteCoordinator.processChunks(topologyInfo,
                        mockRemoteIterator(createChunk(15, EntityType.VIRTUAL_MACHINE)));
    }

    /**
     * Checks that {@link StatsWriteCoordinator} splits properly several big chunks into subchunks.
     *
     * @throws InterruptedException in case test thread will be interrupted
     * @throws TimeoutException in case data receiving process will not be completed
     *                 in desired amount of time.
     * @throws CommunicationException in case data receiving process will fail due
     *                 to connection issues.
     */
    @Test
    public void checkTwoBigEntitiesChunks()
                    throws InterruptedException, TimeoutException, CommunicationException {
        final int vmsAmount = 15;
        final int pmsAmount = 8;
        final int storagesAmount = 13;
        final StatsWriteCoordinator statsWriteCoordinator = new StatsWriteCoordinator(statsWritersPool, Collections.singleton(
                        new TestChunkedTopologyWriter(5, MAX_WRITE_TOPOLOGY_CHUNK_SIZE, 8, 3,
                                        MAX_WRITE_TOPOLOGY_CHUNK_SIZE)), Collections.singleton(
                        new TestCompleteTopologyWriter(vmsAmount + pmsAmount + storagesAmount)),
                        MAX_WRITE_TOPOLOGY_CHUNK_SIZE);
        final Collection<TopologyEntityDTO> vmsChunk =
                        createChunk(vmsAmount, EntityType.VIRTUAL_MACHINE);
        final Collection<TopologyEntityDTO> pmsChunk =
                        createChunk(pmsAmount, EntityType.PHYSICAL_MACHINE);
        final Collection<TopologyEntityDTO> storagesChunk =
                        createChunk(storagesAmount, EntityType.STORAGE);
        statsWriteCoordinator.processChunks(topologyInfo,
                        mockRemoteIterator(vmsChunk, pmsChunk, storagesChunk));
    }

    /**
     * Checks that {@link StatsWriteCoordinator} will not fail in case one of the scheduled tasks for chunked
     * topology writers will fail.
     *
     * @throws InterruptedException in case test thread will be interrupted
     * @throws TimeoutException in case data receiving process will not be completed
     *                 in desired amount of time.
     * @throws CommunicationException in case data receiving process will fail due
     *                 to connection issues.
     */
    @Test
    public void checkOneChunkedTopologyWritingTaskFailure()
                    throws InterruptedException, TimeoutException, CommunicationException {
        final int vmsAmount = 15;
        final int pmsAmount = 8;
        final int storagesAmount = 13;
        final StatsWriteCoordinator statsWriteCoordinator = new StatsWriteCoordinator(statsWritersPool, Collections.singleton(
                        new TestChunkedTopologyWriter(
                                        Collections.singletonMap(5, EXPECTED_CHUNK_EXCEPTION), 5,
                                        MAX_WRITE_TOPOLOGY_CHUNK_SIZE, 8, 3,
                                        MAX_WRITE_TOPOLOGY_CHUNK_SIZE)), Collections.singleton(
                        new TestCompleteTopologyWriter(vmsAmount + pmsAmount + storagesAmount)),
                        MAX_WRITE_TOPOLOGY_CHUNK_SIZE);
        final Collection<TopologyEntityDTO> vmsChunk =
                        createChunk(vmsAmount, EntityType.VIRTUAL_MACHINE);
        final Collection<TopologyEntityDTO> pmsChunk =
                        createChunk(pmsAmount, EntityType.PHYSICAL_MACHINE);
        final Collection<TopologyEntityDTO> storagesChunk =
                        createChunk(storagesAmount, EntityType.STORAGE);
        statsWriteCoordinator.processChunks(topologyInfo,
                        mockRemoteIterator(vmsChunk, pmsChunk, storagesChunk));
    }

    /**
     * Checks that {@link StatsWriteCoordinator} will not fail in case one of the scheduled tasks for complete
     * topology writers will fail.
     *
     * @throws InterruptedException in case test thread will be interrupted
     * @throws TimeoutException in case data receiving process will not be completed
     *                 in desired amount of time.
     * @throws CommunicationException in case data receiving process will fail due
     *                 to connection issues.
     */
    @Test
    public void checkOneCompleteTopologyWritingTaskFailure()
                    throws InterruptedException, TimeoutException, CommunicationException {
        final int vmsAmount = 15;
        final int pmsAmount = 8;
        final int storagesAmount = 13;
        final int allEntitiesNumber = vmsAmount + pmsAmount + storagesAmount;
        final StatsWriteCoordinator statsWriteCoordinator = new StatsWriteCoordinator(statsWritersPool, Collections.singleton(
                        new TestChunkedTopologyWriter(5, MAX_WRITE_TOPOLOGY_CHUNK_SIZE, 8, 3,
                                        MAX_WRITE_TOPOLOGY_CHUNK_SIZE)),
                        ImmutableSet.of(new TestCompleteTopologyWriter(allEntitiesNumber),
                                        new TestCompleteTopologyWriter(
                                                        Collections.singletonMap(allEntitiesNumber,
                                                                        EXPECTED_COMPLETE_EXCEPTION),
                                                        0)), MAX_WRITE_TOPOLOGY_CHUNK_SIZE);
        final Collection<TopologyEntityDTO> vmsChunk =
                        createChunk(vmsAmount, EntityType.VIRTUAL_MACHINE);
        final Collection<TopologyEntityDTO> pmsChunk =
                        createChunk(pmsAmount, EntityType.PHYSICAL_MACHINE);
        final Collection<TopologyEntityDTO> storagesChunk =
                        createChunk(storagesAmount, EntityType.STORAGE);
        statsWriteCoordinator.processChunks(topologyInfo,
                        mockRemoteIterator(vmsChunk, pmsChunk, storagesChunk));
    }

    /**
     * Checks that {@link StatsWriteCoordinator} will not fail in case one of the scheduled tasks for complete
     * topology writers will fail and one of the scheduled tasks for chunked topology writers will
     * fail.
     *
     * @throws InterruptedException in case test thread will be interrupted
     * @throws TimeoutException in case data receiving process will not be completed
     *                 in desired amount of time.
     * @throws CommunicationException in case data receiving process will fail due
     *                 to connection issues.
     */
    @Test
    public void checkCompleteAndChunkTaskFailed()
                    throws InterruptedException, TimeoutException, CommunicationException {
        final int vmsAmount = 15;
        final int pmsAmount = 8;
        final int storagesAmount = 13;
        final int allEntitiesNumber = vmsAmount + pmsAmount + storagesAmount;
        final StatsWriteCoordinator statsWriteCoordinator = new StatsWriteCoordinator(statsWritersPool, Collections.singleton(
                        new TestChunkedTopologyWriter(
                                        Collections.singletonMap(5, EXPECTED_CHUNK_EXCEPTION), 5,
                                        MAX_WRITE_TOPOLOGY_CHUNK_SIZE, 8, 3,
                                        MAX_WRITE_TOPOLOGY_CHUNK_SIZE)),
                        ImmutableSet.of(new TestCompleteTopologyWriter(allEntitiesNumber),
                                        new TestCompleteTopologyWriter(
                                                        Collections.singletonMap(allEntitiesNumber,
                                                                        EXPECTED_COMPLETE_EXCEPTION),
                                                        0)), MAX_WRITE_TOPOLOGY_CHUNK_SIZE);
        final Collection<TopologyEntityDTO> vmsChunk =
                        createChunk(vmsAmount, EntityType.VIRTUAL_MACHINE);
        final Collection<TopologyEntityDTO> pmsChunk =
                        createChunk(pmsAmount, EntityType.PHYSICAL_MACHINE);
        final Collection<TopologyEntityDTO> storagesChunk =
                        createChunk(storagesAmount, EntityType.STORAGE);
        statsWriteCoordinator.processChunks(topologyInfo,
                        mockRemoteIterator(vmsChunk, pmsChunk, storagesChunk));
    }

    private static RemoteIterator<TopologyEntityDTO> mockRemoteIterator(
                    Collection<TopologyEntityDTO>... chunks)
                    throws InterruptedException, TimeoutException, CommunicationException {
        final Queue<Collection<TopologyEntityDTO>> chunksQueue =
                        Stream.of(chunks).collect(Collectors.toCollection(LinkedList::new));
        @SuppressWarnings("unchecked")
        final RemoteIterator<TopologyEntityDTO> result = Mockito.mock(RemoteIterator.class);
        Mockito.when(result.hasNext())
                        .thenAnswer((Answer<Boolean>)invocation -> !chunksQueue.isEmpty());
        Mockito.when(result.nextChunk())
                        .thenAnswer((Answer<Collection<TopologyEntityDTO>>)invocation -> chunksQueue
                                        .poll());
        return result;
    }

    private static TopologyEntityDTO createEntity(EntityType type, int oid) {
        return TopologyEntityDTO.newBuilder().setEntityType(type.getNumber()).setOid(oid).build();
    }

    private static Collection<TopologyEntityDTO> createChunk(int entitiesNumber, EntityType type) {
        return IntStream.range(0, entitiesNumber).mapToObj(oid -> createEntity(type, oid))
                        .collect(Collectors.toSet());
    }

    /**
     * Checks that tasks for chunked topology writing will call writers with desired parameters.
     */
    private static class TestChunkedTopologyWriter extends AbstractTestTopologyWriter
                    implements ICompleteTopologyStatsWriter {
        private final Collection<Integer> expectedObjectsSize;

        private TestChunkedTopologyWriter(Integer... expectedObjectsSize) {
            this(Collections.emptyMap(), expectedObjectsSize);
        }

        private TestChunkedTopologyWriter(Map<Integer, VmtDbException> expectedObjectsSizeToFailure,
                        Integer... expectedObjectsSize) {
            super(expectedObjectsSizeToFailure);
            this.expectedObjectsSize = Stream.of(expectedObjectsSize)
                            .collect(Collectors.toCollection(CopyOnWriteArrayList::new));
        }

        @Override
        protected void checkObjectsSize(int result) {
            Assert.assertTrue(
                            String.format("There is no '%s' objects size in expected collection '%s'",
                                            result, expectedObjectsSize),
                            expectedObjectsSize.remove(result));
        }
    }


    /**
     * {@link AbstractTestTopologyWriter} provides a way to throw desired exception in case we are
     * facing topology chunk of specific size.
     */
    private abstract static class AbstractTestTopologyWriter extends AbstractStatsWriter {
        private final Map<Integer, VmtDbException> expectedObjectsSizeToFailure;

        protected AbstractTestTopologyWriter(
                        Map<Integer, VmtDbException> expectedObjectsSizeToFailure) {
            this.expectedObjectsSizeToFailure = expectedObjectsSizeToFailure;
        }

        @Override
        protected int process(@Nonnull TopologyInfo topologyInfo,
                        @Nonnull Collection<TopologyEntityDTO> objects) throws VmtDbException {
            final int result = objects.size();
            final VmtDbException expectedFailure = expectedObjectsSizeToFailure.get(result);
            if (expectedFailure != null) {
                throw expectedFailure;
            }
            checkObjectsSize(result);
            return result;
        }

        protected abstract void checkObjectsSize(int result);
    }


    /**
     * Checks that data writing task calling complete topology writer with correct parameters.
     */
    private static class TestCompleteTopologyWriter extends AbstractTestTopologyWriter
                    implements ICompleteTopologyStatsWriter {
        private final int topologySize;

        private TestCompleteTopologyWriter(int topologySize) {
            this(Collections.emptyMap(), topologySize);
        }

        private TestCompleteTopologyWriter(
                        Map<Integer, VmtDbException> expectedObjectsSizeToFailure,
                        int topologySize) {
            super(expectedObjectsSizeToFailure);
            this.topologySize = topologySize;
        }

        @Override
        protected void checkObjectsSize(int result) {
            Assert.assertThat(result, CoreMatchers.is(topologySize));
        }
    }

}
