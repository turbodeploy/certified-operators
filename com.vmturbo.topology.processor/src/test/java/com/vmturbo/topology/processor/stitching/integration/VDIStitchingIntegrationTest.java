package com.vmturbo.topology.processor.stitching.integration;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.sdkDtosFromFile;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.junit.After;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;
import com.vmturbo.stitching.vdi.DesktopPoolMasterImageStitchingOperation;
import com.vmturbo.stitching.vdi.PMStitchingOperation;
import com.vmturbo.stitching.vdi.VDIStitchingOperation;
import com.vmturbo.stitching.vdi.StorageStitchingOperation;
import com.vmturbo.stitching.vdi.VDCStitchingOperation;
import com.vmturbo.stitching.vdi.VMStitchingOperation;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingIntegrationTest;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Test class for the VDI stitching operations tests.
 */
public class VDIStitchingIntegrationTest extends StitchingIntegrationTest {


    private final long vcProbeId = 1111L;
    private final long vcTargetId = 2222L;
    private final long vdiTargetId = 3333L;
    private StitchingManager stitchingManager;
    private StitchingContext stitchingContext;
    private IStitchingJournal<StitchingEntity> journal;
    private StringBuilder journalStringBuilder;

    @Override
    public void setup() throws Exception {
        super.setup();
        final Map<Long, EntityDTO> vdiEntities =
                sdkDtosFromFile(getClass(), "protobuf/messages/vdi_stitch_test.json", 1L);
        final Map<Long, EntityDTO> vcEntities =
                sdkDtosFromFile(getClass(), "protobuf/messages/vcenter_vdi_stitch_test.json", vdiEntities.size() + 1L);

        addEntities(vdiEntities, vdiTargetId);
        addEntities(vcEntities, vcTargetId);
        setOperationsForProbe(vcProbeId, Collections.emptyList());

        stitchingManager = new StitchingManager(stitchingOperationStore,
                preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore,
                cpuCapacityStore);

        final Target vdiTarget = mock(Target.class);
        when(vdiTarget.getId()).thenReturn(vdiTargetId);

        when(targetStore.getProbeTargets(vdiTargetId))
                .thenReturn(Collections.singletonList(vdiTarget));


        final Target vcTarget = mock(Target.class);
        when(vcTarget.getId()).thenReturn(vcTargetId);

        when(targetStore.getProbeTargets(vcProbeId))
                .thenReturn(Collections.singletonList(vcTarget));

        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERVISOR))
                .thenReturn(Collections.singletonList(vcProbeId));
        when(probeStore.getProbeIdForType(SDKProbeType.VCENTER.getProbeType()))
                .thenReturn(Optional.of(vcProbeId));
        when(probeStore.getProbeIdForType(SDKProbeType.VCENTER.name()))
                .thenReturn(Optional.of(vcProbeId));

        journalStringBuilder = new StringBuilder(2048);

        final ConfigurableStitchingJournalFactory journalFactory = StitchingJournalFactory
                .configurableStitchingJournalFactory(Clock.systemUTC())
                .addRecorder(new StringBuilderRecorder(journalStringBuilder));
        journalFactory.setJournalOptions(JournalOptions.newBuilder()
                .setVerbosity(Verbosity.LOCAL_CONTEXT_VERBOSITY)
                .build());

        stitchingContext = entityStore.constructStitchingContext();

        journal = journalFactory.stitchingJournal(stitchingContext);

    }

    @After
    public void tearDown() {
        journalStringBuilder.delete(0, journalStringBuilder.length() - 1);
    }

    private String testVDIStitching(VDIStitchingOperation vdiStitching) throws Exception {

        setOperationsForProbe(vdiTargetId,
                Collections.singletonList(vdiStitching));

        stitchingManager.stitch(stitchingContext, journal);
        final Map<Long, TopologyEntity.Builder> topology = stitchingContext.constructTopology();


        return journalStringBuilder.toString();
    }

    /**
     * Tests the VDI proxy VDC and VC VDC stitch operation
     * @throws Exception
     */
    @Test
    public void testVDCStitching() throws Exception {

        VDIStitchingOperation vdiVdcStitchingOperation = new VDCStitchingOperation();

        final String journalOutput = testVDIStitching(vdiVdcStitchingOperation);

        assertThat(journalOutput, containsString(
                "Merging from VIRTUAL_DATACENTER-5-SITE02-POD01-POOL04 onto"));
        assertThat(journalOutput, containsString(
                "VIRTUAL_DATACENTER-8-SITE02-POD01-POOL04"));
        assertThat(journalOutput, containsString(
                "Providers: removed=[VIRTUAL_DATACENTER-5-3333] added=[VIRTUAL_DATACENTER-8-2222]"));

    }

    /**
     * Tests the VDI proxy Storage and VC Storage stitch operation.
     * @throws Exception
     */
    @Test
    public void testStorageStitching() throws Exception {

        VDIStitchingOperation vdiStorageStitchingOperation = new StorageStitchingOperation();

        final String journalOutput = testVDIStitching(vdiStorageStitchingOperation);


        assertThat(journalOutput, containsString(
                "Merging from STORAGE-3-QS4:ESXDC25DS1 onto STORAGE-10-QS4:ESXDC25DS1"));
        assertThat(journalOutput, containsString(
                "STORAGE fb5f0bc9-a0c52693 QS4:ESXDC25DS1"));
        assertThat(journalOutput, containsString(
                "REMOVED ENTITY"));
        assertThat(journalOutput, containsString(
                "[STORAGE e4385f03169a6a1d6573afcb6b45bf2ffd74084c" +
                        " QS4:ESXDC25DS1 (oid-3 tgt-3333)]"));

    }

    /**
     * Tests the VDI proxy PM and VC PM stitch operation.
     * @throws Exception
     */
    @Test
    public void testPMStitching() throws Exception {

        VDIStitchingOperation vdiPMStitchingOperation = new PMStitchingOperation();

        final String journalOutput = testVDIStitching(vdiPMStitchingOperation);

        System.out.println(journalOutput);

        assertThat(journalOutput, containsString(
                "Merging from PHYSICAL_MACHINE-7-hp-esx113.eng.vmturbo.com onto"));
        assertThat(journalOutput, containsString(
                " PHYSICAL_MACHINE-12-hp-esx113.eng.vmturbo.com"));
                assertThat(journalOutput, containsString("++++\"key\": \"PhysicalMachine" +
                        "::6521b6717ffed86e9e7a3d541f0537b9bc52b548"));
        assertThat(journalOutput, containsString(
                "REMOVED ENTITY"));
        assertThat(journalOutput, containsString(
                "[PHYSICAL_MACHINE 6521b6717ffed86e9e7a3d541f0537b9bc52b548" +
                        " hp-esx113.eng.vmturbo.com (oid-7 tgt-3333)]"));



    }

    /**
     * Tests the VDI proxy VM and VC VM stitch operation.
     * @throws Exception
     */
    @Test
    public void testVMStitching() throws Exception {

        VDIStitchingOperation vdiVdcStitchingOperation = new VMStitchingOperation();

        final String journalOutput = testVDIStitching(vdiVdcStitchingOperation);

        assertThat(journalOutput, containsString(
                "VIRTUAL_MACHINE-9-S02-P01-P04-05"));
        assertThat(journalOutput, containsString(
                " Updating entity properties for BUSINESS_USER-1-horizon-user-81"));
        assertThat(journalOutput, containsString(
                "virtualMachine\": ((\"a80721619b6fbb99ea5e7b61f6956976d1c22e67\" --> \"9\"))"));

        assertThat(journalOutput, containsString("4/4 changesets"));
        assertThat(journalOutput, containsString(
                "REMOVED ENTITY"));
        assertThat(journalOutput, containsString(
                "[VIRTUAL_MACHINE a80721619b6fbb99ea5e7b61f6956976d1c22e67" +
                        " S02-P01-P04-05 (oid-2 tgt-3333)]"));
    }

    /**
     * Tests the VDI proxy Desktop Pool and VC VM stitch operation for master image
     * uid to OID resolution.
     * @throws Exception
     */
    @Test
    public void testMasterImageStitching() throws Exception {
        DesktopPoolMasterImageStitchingOperation vdiMasterImageStitchingOperation =
                new DesktopPoolMasterImageStitchingOperation();

        setOperationsForProbe(vdiTargetId,
                Collections.singletonList(vdiMasterImageStitchingOperation));

        stitchingManager.stitch(stitchingContext, journal);
        final Map<Long, TopologyEntity.Builder> topology = stitchingContext.constructTopology();

        final String journalOutput = journalStringBuilder.toString();

        assertThat(journalOutput, containsString(
                "DESKTOP_POOL f31dfba7b0c24783cdb240889f5ca293bc7f8c0c SITE02-POD01-POOL04-LINKED-SMALL-1VCPU-2GB (oid-4 tgt-3333)"));
        assertThat(journalOutput, containsString(
                "masterImage\": ((\"4233fb65-8d8e-31fe-90c8-8a89369d5072\" --> \"9\"))"));

        assertThat(journalOutput, containsString(
                "++++++\"name\": \"masterImageSource" ));
        assertThat(journalOutput, containsString(
                "++++++\"value\": \"VIRTUAL_MACHINE" ));

    }

}