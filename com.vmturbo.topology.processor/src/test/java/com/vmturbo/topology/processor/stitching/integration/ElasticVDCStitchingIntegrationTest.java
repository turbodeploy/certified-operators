package com.vmturbo.topology.processor.stitching.integration;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.sdkDtosFromFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;
import com.vmturbo.stitching.vcd.ElasticVDCStitchingOperation;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingIntegrationTest;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Attempt to simulate elastic VDC stitching operation.
 */
public class ElasticVDCStitchingIntegrationTest extends StitchingIntegrationTest {

    private final long vcProbeId = 1111L;
    private final long vcTargetId = 2222L;
    private final long vcdTargetId = 7777L;

    @Test
    public void testElasticVDCStitching() throws Exception {
        testElasticVDCStitching(new ElasticVDCStitchingOperation());
    }

    /**
     * Test for elastic VDC stitching. The Entity DTOs using in the test will be the following.
     *
     * Before stitching:
     *      VM1  VM2     VM3
     *       \    /       |
     *     Cons VDC1  Cons VDC2    <-  [E Cons VDC1]
     *         |          |
     *     Prod VDC1  Prod VDC2    <-  [E Prod VDC1]
     *         |          |
     *        PM1        PM2
     *
     * After stitching:
     *      VM1   VM2   VM3
     *         \   |    /
     *         E Cons VDC1
     *             |
     *         E Prod VDC1
     *           /    \
     *        PM1     PM2
     *
     * @param vdcStitchingOperationToTest Instance of VDCStitchingOperation.
     * @throws Exception
     */
    private void testElasticVDCStitching(StitchingOperation vdcStitchingOperationToTest) throws Exception {
        final Map<Long, EntityDTO> vcdEntities =
                sdkDtosFromFile(getClass(), "protobuf/messages/elastic_vdc_stitching-vcd_dto.json", 1L);
        final Map<Long, EntityDTO> hypervisorEntities =
                sdkDtosFromFile(getClass(), "protobuf/messages/elastic_vdc_stitching-vcenter_dto.json", vcdEntities.size() + 1L);

        addEntities(vcdEntities, vcdTargetId);
        addEntities(hypervisorEntities, vcTargetId);

        setOperationsForProbe(vcdTargetId,
                Collections.singletonList(vdcStitchingOperationToTest));
        setOperationsForProbe(vcProbeId, Collections.emptyList());

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
                preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore,
                cpuCapacityStore);
        final Target vcdTarget = mock(Target.class);
        when(vcdTarget.getId()).thenReturn(vcdTargetId);

        when(targetStore.getProbeTargets(vcdTargetId))
                .thenReturn(Collections.singletonList(vcdTarget));
        final Target vcTarget = mock(Target.class);
        when(vcTarget.getId()).thenReturn(vcTargetId);

        when(targetStore.getProbeTargets(vcProbeId))
                .thenReturn(Collections.singletonList(vcTarget));

        when(probeStore.getProbeIdsForCategory(ProbeCategory.CLOUD_MANAGEMENT))
                .thenReturn(Collections.singletonList(vcdTargetId));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERVISOR))
                .thenReturn(Collections.singletonList(vcProbeId));
        when(probeStore.getProbeIdForType(SDKProbeType.VCENTER.getProbeType()))
                .thenReturn(Optional.of(vcProbeId));

        final StringBuilder journalStringBuilder = new StringBuilder(2048);
        final StitchingContext stitchingContext = entityStore.constructStitchingContext();

        final ConfigurableStitchingJournalFactory journalFactory = StitchingJournalFactory
                .configurableStitchingJournalFactory(Clock.systemUTC())
                .addRecorder(new StringBuilderRecorder(journalStringBuilder));
        journalFactory.setJournalOptions(JournalOptions.newBuilder()
                .setVerbosity(Verbosity.LOCAL_CONTEXT_VERBOSITY)
                .build());
        final IStitchingJournal<StitchingEntity> journal = journalFactory.stitchingJournal(stitchingContext);
        stitchingManager.stitch(stitchingContext, journal);
        final Map<Long, TopologyEntityDTO.Builder> topology = stitchingContext.constructTopology();

        // System should have found the following stitching points:

        //     REMOVED                   RETAINED
        // --------------------------------------------------------
        // VC-ConsumerVDC-1         VCD-ConsumerVDC-1
        // VC-ConsumerVDC-2         VCD-ConsumerVDC-1
        // VC-ProducerVDC-1         VCD-ProducerVDC-1
        // VC-ProducerVDC-2         VCD-ProducerVDC-1
        final List<Long> stitchingVDCOids = oidsFor(
                Stream.of("VC-ConsumerVDC-1", "VC-ConsumerVDC-2",
                        "VC-ProducerVDC-1", "VC-ProducerVDC-2"),
                hypervisorEntities);
        final List<Long> redundantVDCOids = oidsFor(Stream.of("VC-VM-redundant"), hypervisorEntities);
        final List<Long> elasticConsumerVDCOids = oidsFor(
                Stream.of("VCD-ConsumerVDC-1"),
                vcdEntities);
        final List<Long> elasticProducerVDCOids = oidsFor(
                Stream.of("VCD-ProducerVDC-1"),
                vcdEntities);

        // Stitching VDCs from VC should be removed
        stitchingVDCOids.forEach(oid -> assertNull(topology.get(oid)));
        // Redundant VDCs from VC should not be removed
        redundantVDCOids.forEach(oid -> assertNotNull(topology.get(oid)));

        final List<StitchingEntity> elasticConsumerVDC = stitchingContext.getStitchingGraph().entities()
                .filter(entity -> elasticConsumerVDCOids.contains(entity.getOid()))
                .collect(Collectors.toList());
        // Should be 1 elastic consumer VDC
        assertEquals(1, elasticConsumerVDC.size());
        final List<StitchingEntity> vms = elasticConsumerVDC.get(0).getConsumers().stream()
                .collect(Collectors.toList());
        // 3 VMs consumes from elastic consumer VDC
        assertEquals(3, vms.size());
        vms.forEach(vm -> {
            // No VM provider from original VDCs
            assertTrue(!vm.getProviders().stream()
                    .anyMatch(provider -> stitchingVDCOids.contains(provider.getOid())));
            // There is VM provider from elastic VDC
            assertTrue(vm.getProviders().stream()
                    .anyMatch(provider -> provider.getOid() == elasticConsumerVDC.get(0).getOid()));
        });

        final List<StitchingEntity> elasticProducerVDC = stitchingContext.getStitchingGraph().entities()
                .filter(entity -> elasticProducerVDCOids.contains(entity.getOid()))
                .collect(Collectors.toList());
        // Should be 1 elastic producer VDC
        assertEquals(1, elasticProducerVDC.size());
        // Should have 2 PMs as provider
        assertEquals(2, elasticProducerVDC.get(0).getProviders().size());
        // Verify all 6 commodities bought from VC VDCs has been moved to elastic producer VDCs
        assertEquals(6, elasticProducerVDC.get(0).getCommodityBoughtListByProvider()
                .values().stream()
                .flatMap(List::stream)
                .flatMap(commBought -> commBought.getBoughtList().stream())
                .count());
        // Elastic consumer VDC still consume from elastic producer VDC
        assertTrue(elasticProducerVDC.get(0).getConsumers().stream()
                .anyMatch(consumer -> consumer == elasticConsumerVDC.get(0)));
    }
 }
