package com.vmturbo.topology.processor.stitching.integration;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.sdkDtosFromFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cloudfoundry.CloudFoundryVMStitchingOperation;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingIntegrationTest;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;


/**
 * Attempt to simulate Cloud foundry probe stitching operation.
 */
public class CloudfoundryStitchingIntegrationTest extends StitchingIntegrationTest {

    private final long vcProbeId = 1111L;
    private final long vcTargetId = 2222L;
    private final long cloudFoundryProbeId = 3333L;
    private final long cloudFoundryTargetId = 4444L;

    @Test
    public void testCloudfoundryStitching() throws Exception {
        testCloudfoundryStitching(new CloudFoundryVMStitchingOperation());
    }

    private void testCloudfoundryStitching(StitchingOperation vmStitchingOperationToTest)
            throws Exception {
        final Map<Long, EntityDTO> cloudfoundryEntities =
                sdkDtosFromFile(getClass(), "protobuf/messages/cloudfoundry_cf_data.json",
                        1L);
        final Map<Long, EntityDTO> hypervisorEntities =
                sdkDtosFromFile(getClass(), "protobuf/messages/cloudfoundry_vc_data.json",
                        cloudfoundryEntities.size() + 1L);

        addEntities(cloudfoundryEntities, cloudFoundryTargetId);
        addEntities(hypervisorEntities, vcTargetId);

        setOperationsForProbe(cloudFoundryProbeId,
                Collections.singletonList(vmStitchingOperationToTest));
        setOperationsForProbe(vcProbeId, Collections.emptyList());

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
                preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore,
                targetStore, cpuCapacityStore);
        final Target apmTarget = mock(Target.class);
        when(apmTarget.getId()).thenReturn(cloudFoundryTargetId);
        final Target vcTarget = mock(Target.class);
        when(vcTarget.getId()).thenReturn(vcTargetId);
        when(targetStore.getProbeTargets(cloudFoundryProbeId))
                .thenReturn(Collections.singletonList(apmTarget));
        when(targetStore.getProbeTargets(vcProbeId))
                .thenReturn(Collections.singletonList(vcTarget));
        when(probeStore.getProbe(cloudFoundryProbeId)).thenReturn(Optional.empty());
        when(probeStore.getProbeIdsForCategory(ProbeCategory.PAAS))
                .thenReturn(Collections.singletonList(cloudFoundryProbeId));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERVISOR))
                .thenReturn(Collections.singletonList(vcProbeId));

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
        final Map<Long, TopologyEntity.Builder> topology = stitchingContext.constructTopology();
        // these proxy VMs that matched stitched with hypervisor VMs should have been removed.
        final List<Long> cfExpectedRemoved = oidsFor(Stream.of("vm-1"), cloudfoundryEntities);
        // these unmatched VMs should have been retained.
        final List<Long> cfExpectedRetained = oidsFor(Stream.of("vm-2"), cloudfoundryEntities);
        // all entities from VC should be retained.
        final List<Long> vcExpectedRetained = oidsFor(Stream.of("vm-1", "vm-2"),
                hypervisorEntities);

        cfExpectedRemoved.forEach(oid -> assertNull(topology.get(oid)));
        cfExpectedRetained.forEach(oid -> assertNotNull(topology.get(oid)));
        vcExpectedRetained.forEach(oid -> assertNotNull(topology.get(oid)));

        final long vm1Oid = vcExpectedRetained.get(0);
        final long proxyVm1Oid = cfExpectedRemoved.get(0);
        final double delta = 1e-7;
        // get proxy entity from entity store, and real entity from repository
        final EntityDTO proxyVm1 = entityStore.getEntity(proxyVm1Oid).get()
                .getEntityInfo(cloudFoundryTargetId).get().getEntityInfo();
        final TopologyEntity vm1Topo = topology.get(vm1Oid).build();

        // verify commodities sold in VM-1 has been replaced by proxy in VM-1
        vm1Topo.getTopologyEntityDtoBuilder().getCommoditySoldListList().forEach(
                commoditySoldDTO -> {
                    final int commTypeVal = commoditySoldDTO.getCommodityType().getType();
                    // get commodity sold DTO in proxy VM
                    final CommodityDTO proxyComm = proxyVm1.getCommoditiesSoldList().stream()
                            .filter(commSold ->
                                    commSold.getCommodityType().getNumber() == commTypeVal)
                            .findFirst().get();
                    assertEquals(proxyComm.getUsed(), commoditySoldDTO.getUsed(), delta);
                    assertEquals(proxyComm.getPeak(), commoditySoldDTO.getPeak(), delta);
                    assertEquals(proxyComm.getCapacity(), commoditySoldDTO.getCapacity(), delta);
                }
        );
    }
 }
