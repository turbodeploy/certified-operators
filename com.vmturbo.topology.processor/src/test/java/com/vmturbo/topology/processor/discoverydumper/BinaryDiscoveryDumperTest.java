package com.vmturbo.topology.processor.discoverydumper;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.google.common.collect.Streams;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.components.common.diagnostics.ZipStreamBuilder;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;

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
    private BinaryDiscoveryDumper dumper;

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

    /**
     * Tests that discovery dumps are correctly written in the diags. They should be placed in the
     * dumper.getDiagsBinaryDiscoveriesFolder() folder and there should be one file per discovery
     * dump.
     *
     * @throws InterruptedException if an error occurs
     * @throws IOException if an I/O error occurs
     */
    @Test
    public void testWriteDiscoveryDumpsToStream() throws IOException, InterruptedException {
        dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        dumpDiscovery(TGT_ID_2, DiscoveryType.FULL);
        ByteArrayOutputStream zipBytes = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(zipBytes);
        dumper.dumpToStream(zos);
        ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes.toByteArray()));

        List<File> discoveryResponses = dumper.getBinaryFiles();
        for (File file : discoveryResponses) {
            ZipEntry ze = zis.getNextEntry();
            if (ze.isDirectory()) {
                ze = zis.getNextEntry();
            }
            assertEquals(file.getName(), new File(ze.getName()).getName());
        }
    }

    /**
     * Tests that discovery dumps are correctly copied from the diags. They should be placed in the
     * components volume, defined in the BinaryDiscoveryDumper.
     *
     * @throws InterruptedException not expected to happen
     * @throws IOException if an I/O error occurs
     */
    @Test
    public void testWriteCopyFunction() throws IOException, InterruptedException {
        String discoveryDumpFilePath = "706923583886376-2020.08.24.15.51.39.855-FULL.dto.lz4";
        ZipStreamBuilder builder = ZipStreamBuilder.builder()
            .withTextFile("/BinaryDiscoveries/" + discoveryDumpFilePath);
        DiagsZipReader diags = new DiagsZipReader(builder.toInputStream(), dumper, true);
        Streams.stream(diags).collect(Collectors.toList());
        List<File> files = dumper.getBinaryFiles();
        Assert.assertEquals(1, files.size());
        Assert.assertEquals(discoveryDumpFilePath, files.get(0).getName());

    }

    private void assertNumberOfDiscoveryDumps(int expectedNumberOfDiscoveryDumps) {
        final String[] dumpFilenames = dumpDir.list();
        Assert.assertNotNull(dumpFilenames);
        Assert.assertEquals(expectedNumberOfDiscoveryDumps, dumpFilenames.length);
    }
}
