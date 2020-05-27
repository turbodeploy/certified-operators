package com.vmturbo.topology.processor.discoverydumper;

import java.io.File;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.sdk.server.common.DiscoveryDumper;

/**
 * Unit test for dump files repository {@link DiscoveryDumperImpl}.
 */
public class BinaryDiscoveryDumperTest {

    private static final String TGT_ID = "1";
    private static final String TGT_ID_2 = "2";

    /**
     * Temporary folder with cached responses for testing.
     */
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();
    private File dumpDir;
    private DiscoveryDumper dumper;

    /**
     * Initialize variables for tests.
     * @throws Exception if an error occurs
     */
    @Before
    public void init() throws Exception {
        dumpDir = new File(tmpFolder.newFolder("dump-root"), "dumps");
        dumper = new BinaryDiscoveryDumper(dumpDir);
    }


    /**
     * Tests that dumping discovery works, and we only have one discovery dump file per target.
     * @throws InterruptedException if an error occurs
     */
    @Test
    public void testDifferentTargetsDumping() throws InterruptedException {
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        dumpDiscovery(TGT_ID_2, DiscoveryType.FULL);
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        dumpDiscovery(TGT_ID_2, DiscoveryType.FULL);
        assertNumberOfDiscoveryDumps(2);

    }


    private void dumpDiscovery(String target, DiscoveryType discoveryType)
          throws InterruptedException {
        dumper.dumpDiscovery(
            target,
            discoveryType,
            DiscoveryResponse.newBuilder().addErrorDTO(
                    ErrorDTO.newBuilder()
                        .setDescription("").setSeverity(ErrorDTO.ErrorSeverity.WARNING).build())
                .build(),
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
