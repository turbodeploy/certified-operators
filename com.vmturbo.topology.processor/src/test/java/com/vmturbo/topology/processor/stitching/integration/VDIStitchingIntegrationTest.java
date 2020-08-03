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

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
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
import com.vmturbo.stitching.vdi.VDIPMStitchingOperation;
import com.vmturbo.stitching.vdi.VDIStitchingOperation;
import com.vmturbo.stitching.vdi.VDIStorageStitchingOperation;
import com.vmturbo.stitching.vdi.VDIVDCStitchingOperation;
import com.vmturbo.stitching.vdi.VDIVMStitchingOperation;
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

    private static final long VC_PROBE_ID = 1111L;
    private static final long VC_TARGET_ID = 2222L;
    private static final long VDI_TARGET_ID = 3333L;

    private StitchingManager stitchingManager;
    private StitchingContext stitchingContext;
    private IStitchingJournal<StitchingEntity> journal;
    private StringBuilder journalStringBuilder;

    /**
     * Common pre-test setup. Runs after {@link StitchingIntegrationTest#integrationSetup()}.
     *
     * @throws Exception If anything goes wrong.
     */
    @Before
    public void setup() throws Exception {
        final Map<Long, EntityDTO> vdiEntities =
                sdkDtosFromFile(getClass(), "protobuf/messages/vdi_stitch_test.json", 1L);
        final Map<Long, EntityDTO> vcEntities =
                sdkDtosFromFile(getClass(), "protobuf/messages/vcenter_vdi_stitch_test.json",
                        vdiEntities.size() + 1L);

        addEntities(vdiEntities, VDI_TARGET_ID);
        addEntities(vcEntities, VC_TARGET_ID);
        setOperationsForProbe(VC_PROBE_ID, Collections.emptyList());

        stitchingManager =
                new StitchingManager(stitchingOperationStore, preStitchingOperationLibrary,
                        postStitchingOperationLibrary, probeStore, targetStore, cpuCapacityStore);

        final Target vdiTarget = mock(Target.class);
        when(vdiTarget.getId()).thenReturn(VDI_TARGET_ID);

        when(targetStore.getProbeTargets(VDI_TARGET_ID)).thenReturn(
                Collections.singletonList(vdiTarget));

        final Target vcTarget = mock(Target.class);
        when(vcTarget.getId()).thenReturn(VC_TARGET_ID);

        when(targetStore.getProbeTargets(VC_PROBE_ID)).thenReturn(
                Collections.singletonList(vcTarget));

        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERVISOR)).thenReturn(
                Collections.singletonList(VC_PROBE_ID));
        when(probeStore.getProbeIdForType(SDKProbeType.VCENTER.getProbeType())).thenReturn(
                Optional.of(VC_PROBE_ID));

        journalStringBuilder = new StringBuilder(2048);

        final ConfigurableStitchingJournalFactory journalFactory =
                StitchingJournalFactory.configurableStitchingJournalFactory(Clock.systemUTC())
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

    private String testVDIStitching(VDIStitchingOperation vdiStitching) {

        setOperationsForProbe(VDI_TARGET_ID, Collections.singletonList(vdiStitching));

        stitchingManager.stitch(stitchingContext, journal);
        final Map<Long, TopologyEntity.Builder> topology = stitchingContext.constructTopology();

        return journalStringBuilder.toString();
    }

    /**
     * Tests the VDI proxy VDC and VC VDC stitch operation
     *
     * @throws Exception
     */
    @Test
    public void testVDCStitching() {

        VDIStitchingOperation vdiVdcStitchingOperation = new VDIVDCStitchingOperation();

        final String journalOutput = testVDIStitching(vdiVdcStitchingOperation);

        assertThat(journalOutput,
                containsString("Merging from VIRTUAL_DATACENTER-5-SITE02-POD01-POOL04 onto"));
        assertThat(journalOutput, containsString("VIRTUAL_DATACENTER-8-SITE02-POD01-POOL04"));
        assertThat(journalOutput, containsString(
                "Providers: removed=[VIRTUAL_DATACENTER-5-3333] added=[VIRTUAL_DATACENTER-8-2222]"));
    }

    /**
     * Tests the VDI proxy Storage and VC Storage stitch operation.
     *
     * @throws Exception
     */
    @Test
    public void testStorageStitching() {

        VDIStitchingOperation vdiStorageStitchingOperation = new VDIStorageStitchingOperation();

        final String journalOutput = testVDIStitching(vdiStorageStitchingOperation);

        assertThat(journalOutput, containsString(
                "Merging from STORAGE-3-QS4:ESXDC25DS1 onto STORAGE-10-QS4:ESXDC25DS1"));
        assertThat(journalOutput, containsString("STORAGE fb5f0bc9-a0c52693 QS4:ESXDC25DS1"));
        assertThat(journalOutput, containsString("REMOVED ENTITY"));
        assertThat(journalOutput, containsString(
                "[STORAGE e4385f03169a6a1d6573afcb6b45bf2ffd74084c" +
                        " QS4:ESXDC25DS1 (oid-3 tgt-3333)]"));
        assertThat(journalOutput, CoreMatchers.allOf(containsString(
                "++++++\"key\": \"Storage::e4385f03169a6a1d6573afcb6b45bf2ffd74084c\""),
                containsString(
                        "------\"key\": \"Storage::e4385f03169a6a1d6573afcb6b45bf2ffd74084c\""),
                containsString(
                        "++++\"key\": \"DesktopPool::2396e4cc2d3588611d28e3af3a367ea239afbcae\""),
                containsString(
                        "++++\"key\": \"DesktopPool::30137c008aac7a639c67eec1eec83be28e579855\""),
                containsString(
                        "++++\"key\": \"DesktopPool::6a7708a3c267d3c4197b7100f7956119689537b3\""),
                containsString(
                        "++++\"key\": \"DesktopPool::90d8fa0ea479561108fcb6343fcb9f2e1d3744f5\""),
                containsString(
                        "++++\"key\": \"DesktopPool::b29c7664c9a592a9951325c8261c13295cab7e4d\""),
                containsString(
                        "++++\"key\": \"DesktopPool::b29c7664c9a592a9951325c8261c13295cab7e4d\""),
                containsString(
                        "++++\"key\": \"DesktopPool::d3061c7fd29a3fbee355856859e2a44aba3a5670\""),
                containsString(
                        "++++\"key\": \"DesktopPool::da593e7c92e5bd19329663fe36d112e23f6da20d\""),
                containsString(
                        "++++\"key\": \"DesktopPool::f31dfba7b0c24783cdb240889f5ca293bc7f8c0c\""),
                containsString(
                        "++++\"key\": \"DesktopPool::f31dfba7b0c24783cdb240889f5ca293bc7f8c0c\""),
                containsString(
                        "++++\"key\": \"Storage::e4385f03169a6a1d6573afcb6b45bf2ffd74084c\""),
                containsString(
                        "VIRTUAL_MACHINE a80721619b6fbb99ea5e7b61f6956976d1c22e67 S02-P01-P04-05 (oid-2 tgt-3333)\n" +
                                "  Providers: removed=[STORAGE-3-3333] added=[STORAGE-10-2222]")));
    }

    /**
     * Tests the VDI proxy PM and VC PM stitch operation.
     *
     * @throws Exception
     */
    @Test
    public void testPMStitching() {

        VDIStitchingOperation vdiPMStitchingOperation = new VDIPMStitchingOperation();

        final String journalOutput = testVDIStitching(vdiPMStitchingOperation);

        assertThat(journalOutput,
                containsString("Merging from PHYSICAL_MACHINE-7-hp-esx113.eng.vmturbo.com onto"));
        assertThat(journalOutput, containsString(" PHYSICAL_MACHINE-12-hp-esx113.eng.vmturbo.com"));
        assertThat(journalOutput, containsString(
                "++++\"key\": \"PhysicalMachine" + "::6521b6717ffed86e9e7a3d541f0537b9bc52b548"));
        assertThat(journalOutput, containsString("REMOVED ENTITY"));
        assertThat(journalOutput, containsString(
                "[PHYSICAL_MACHINE 6521b6717ffed86e9e7a3d541f0537b9bc52b548" +
                        " hp-esx113.eng.vmturbo.com (oid-7 tgt-3333)]"));
        assertThat(journalOutput, containsString(
                "VIRTUAL_MACHINE a80721619b6fbb99ea5e7b61f6956976d1c22e67 S02-P01-P04-05 (oid-2 tgt-3333)\n" +
                        "  Providers: removed=[PHYSICAL_MACHINE-7-3333] added=[PHYSICAL_MACHINE-12-2222]"));
    }

    /**
     * Tests the VDI proxy VM and VC VM stitch operation.
     *
     * @throws Exception
     */
    @Test
    public void testVMStitching() {

        VDIStitchingOperation vdiVdcStitchingOperation = new VDIVMStitchingOperation();
        final String journalOutput = testVDIStitching(vdiVdcStitchingOperation);

        assertThat(journalOutput, containsString("VIRTUAL_MACHINE-9-S02-P01-P04-05"));
        assertThat(journalOutput,
                containsString(" Updating entity properties for BUSINESS_USER-1-horizon-user-81"));
        assertThat(journalOutput, containsString(
                "virtualMachine\": ((\"a80721619b6fbb99ea5e7b61f6956976d1c22e67\" --> \"9\"))"));

        assertThat(journalOutput, containsString("4/4 changesets"));
        assertThat(journalOutput, containsString("REMOVED ENTITY"));
        assertThat(journalOutput, containsString(
                "[VIRTUAL_MACHINE a80721619b6fbb99ea5e7b61f6956976d1c22e67" +
                        " S02-P01-P04-05 (oid-2 tgt-3333)]"));

        assertThat(journalOutput,
                CoreMatchers.allOf(containsString("++++++\"commodityType\": \"CLUSTER\""),
                        containsString(
                                "++++++\"key\": \"PhysicalMachine::6521b6717ffed86e9e7a3d541f0537b9bc52b548\""),
                        containsString("++++++\"commodityType\": \"STORAGE_CLUSTER\""),
                        containsString(
                                "++++++\"key\": \"Storage::e4385f03169a6a1d6573afcb6b45bf2ffd74084c\"")));
    }

    /**
     * Tests the VDI proxy Desktop Pool and VC VM stitch operation for master image
     * uid to OID resolution.
     *
     * @throws Exception
     */
    @Test
    public void testMasterImageStitching() {
        DesktopPoolMasterImageStitchingOperation vdiMasterImageStitchingOperation =
                new DesktopPoolMasterImageStitchingOperation();

        setOperationsForProbe(VDI_TARGET_ID,
                Collections.singletonList(vdiMasterImageStitchingOperation));

        stitchingManager.stitch(stitchingContext, journal);
        final Map<Long, TopologyEntity.Builder> topology = stitchingContext.constructTopology();

        final String journalOutput = journalStringBuilder.toString();

        assertThat(journalOutput, containsString(
                "DESKTOP_POOL f31dfba7b0c24783cdb240889f5ca293bc7f8c0c SITE02-POD01-POOL04-LINKED-SMALL-1VCPU-2GB (oid-4 tgt-3333)"));
        assertThat(journalOutput, containsString(
                "masterImage\": ((\"4233fb65-8d8e-31fe-90c8-8a89369d5072\" --> \"9\"))"));
    }
}
