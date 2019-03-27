package com.vmturbo.topology.processor.discoverydumper;

import java.io.File;
import java.util.Collections;
import java.util.Date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.sdk.server.common.DiscoveryDumper;

/**
 * Unit test for dump files repository {@link DiscoveryDumperImpl}.
 */
public class DiscoveryDumperTest {

    private static final String TGT_ID = "http://tricky.target.name";
    private static final String TGT_ID_2 = "One@MoreαβγTarget";

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();
    private TargetDumpingSettings dumpSettings;
    private File dumpDir;
    private DiscoveryDumper dumper;

    @Before
    public void init() throws Exception {
        dumpSettings = Mockito.mock(TargetDumpingSettings.class);
        dumpDir = new File(tmpFolder.newFolder("dump-root"), "dumps");
        dumper = new DiscoveryDumperImpl(dumpDir, dumpSettings);
    }

    /**
     * If dumping is disabled, no dump files are generated and all existing dump files
     * are deleted.
     */
    @Test
    public void testNoDumping() throws InterruptedException {
        Mockito.when(dumpSettings.getDumpsToHold(Mockito.anyString())).thenReturn(1);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        assertNumberOfDiscoveryDumps(1);
        Mockito.when(dumpSettings.getDumpsToHold(Mockito.anyString())).thenReturn(0);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        assertNumberOfDiscoveryDumps(0);
    }

    /**
     * Tests how the dump count is treated. It is expected, that no more than "dumps count" files
     * are created, except the binary and the text version of each discovery.
     */
    @Test
    public void testExactDumping() throws InterruptedException {
        Mockito.when(dumpSettings.getDumpsToHold(Mockito.anyString())).thenReturn(1);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        assertNumberOfDiscoveryDumps(1);

        Mockito.when(dumpSettings.getDumpsToHold(Mockito.anyString())).thenReturn(2);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        assertNumberOfDiscoveryDumps(2);
    }

    /**
     * Tests, how different targets are dumped. It is expected, that number of dumps to save is
     * independent between different targets.
     */
    @Test
    public void testDifferentTargetsDumping() throws InterruptedException {
        Mockito.when(dumpSettings.getDumpsToHold(Mockito.anyString())).thenReturn(1);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        dumpDiscovery(TGT_ID_2, DiscoveryType.FULL);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        dumpDiscovery(TGT_ID_2, DiscoveryType.FULL);
        assertNumberOfDiscoveryDumps(2);

        Mockito.when(dumpSettings.getDumpsToHold(Mockito.anyString())).thenReturn(2);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        assertNumberOfDiscoveryDumps(3);
    }

    /**
     * Tests that if the number of full discovery dumps that exist in the directory is
     * smaller or equal to the maximum number allowed, then no discovery dump is deleted.
     *
     * @param isEqual whether to make the file limit equal to or greater than the number of
     *                full discovery dumps.
     */
    private void testKeepAll(boolean isEqual) throws InterruptedException {
        Mockito.when(dumpSettings.getDumpsToHold(Mockito.anyString())).
            thenReturn(isEqual ? 2 : 3);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        dumpDiscovery(TGT_ID, DiscoveryType.INCREMENTAL);
        dumpDiscovery(TGT_ID, DiscoveryType.PERFORMANCE);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        assertNumberOfDiscoveryDumps(4);
    }

    /**
     * Tests that if the number of full discovery dumps that exist in the directory is
     * equal to the maximum number allowed, then no discovery dump is deleted.
     */
    @Test
    public void testKeepAllEqual() throws InterruptedException {
        testKeepAll(true);
    }

    /**
     * Tests that if the number of full discovery dumps that exist in the directory is
     * less than the maximum number allowed, then no discovery dump is deleted.
     */
    @Test
    public void testKeepAllLess() throws Exception {
        testKeepAll(false);
    }

    /**
     * Tests what happens when a full discovery dump exceeds the number of allowed discovery dumps.
     * The appropriate number of full discoveries must be deleted.  Any non-full discoveries that
     * happened before the oldest full discovery that is still in the directory must be deleted.
     */
    @Test
    public void testDumpingWithDeletion() throws InterruptedException {
        Mockito.when(dumpSettings.getDumpsToHold(Mockito.anyString())).thenReturn(2);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        dumpDiscovery(TGT_ID, DiscoveryType.INCREMENTAL);
        dumpDiscovery(TGT_ID, DiscoveryType.PERFORMANCE);
        dumpDiscovery(TGT_ID, DiscoveryType.INCREMENTAL);
        dumpDiscovery(TGT_ID, DiscoveryType.PERFORMANCE);

        // this is the timestamp of the current moment
        // in the end, all files remaining must be older than this date
        final Date threshold = new Date();

        // this is going to be the last full discovery that stays in the directory
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        dumpDiscovery(TGT_ID, DiscoveryType.INCREMENTAL);
        dumpDiscovery(TGT_ID, DiscoveryType.PERFORMANCE);
        dumpDiscovery(TGT_ID, DiscoveryType.INCREMENTAL);
        dumpDiscovery(TGT_ID, DiscoveryType.PERFORMANCE);

        // this is going to trigger deletion of old files until the threshold date
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);

        assertNumberOfDiscoveryDumps(6);
        for (String fileName : dumpDir.list()) {
            DiscoveryDumpFilename discoveryDumpFilename = DiscoveryDumpFilename.parse(fileName);
            Assert.assertNotNull(discoveryDumpFilename);
            Assert.assertTrue(discoveryDumpFilename.getTimestamp().compareTo(threshold) >= 0);
        }
    }

    /**
     * Empty incremental dumps should be skipped.
     */
    @Test
    public void testEmptyIncrementalDumps() throws InterruptedException {
        Mockito.when(dumpSettings.getDumpsToHold(Mockito.anyString())).thenReturn(1);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        dumpDiscovery(TGT_ID, DiscoveryType.INCREMENTAL);

        // this is an empty incremental dump and should be ignored
        dumpEmptyDiscovery(TGT_ID, DiscoveryType.INCREMENTAL);

        assertNumberOfDiscoveryDumps(2);
    }

    private void dumpDiscovery(String target, DiscoveryType discoveryType)
          throws InterruptedException {
        dumper.dumpDiscovery(
            target,
            discoveryType,
            DiscoveryResponse.newBuilder().addErrorDTO(
                    ErrorDTO.newBuilder().setDescription("").
                    setSeverity(ErrorDTO.ErrorSeverity.WARNING).
                    build()).
                build(),
            Collections.emptyList());

        // ensure dumps don't happen too often:
        // timestamps must differ by at least one millisecond
        Thread.sleep(1L);
    }

    private void dumpEmptyDiscovery(String target, DiscoveryType discoveryType)
          throws InterruptedException {
        dumper.dumpDiscovery(
            target,
            discoveryType,
            DiscoveryResponse.newBuilder().build(),
            Collections.emptyList());

        // ensure dumps don't happen too often:
        // timestamps must differ by at least one millisecond
        Thread.sleep(1L);
    }

    private void assertNumberOfDiscoveryDumps(int expectedNumberOfDiscoveryDumps) {
        final String[] dumpFilenames = dumpDir.list();
        Assert.assertNotNull(dumpFilenames);
        Assert.assertEquals(expectedNumberOfDiscoveryDumps, dumpFilenames.length);
    }
}
