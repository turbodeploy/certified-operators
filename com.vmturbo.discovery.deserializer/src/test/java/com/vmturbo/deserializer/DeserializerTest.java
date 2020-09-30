package com.vmturbo.deserializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.topology.processor.discoverydumper.BinaryDiscoveryDumper;

/**
 * Class to test the deserializer.
 */
public class DeserializerTest {

    private static final String TGT_ID = "1";

    /**
     * Temporary folder with cached responses for testing.
     */
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();
    private BinaryDiscoveryDumper dumper;
    private File dumpDir;


    /**
     * Instantiate temporary folders.
     * @throws Exception if an error occurs
     */
    @Before
    public void init() throws Exception {
        dumpDir = new File(tmpFolder.newFolder("dump-root"), "dumps");
        dumper = new BinaryDiscoveryDumper(dumpDir);
    }

    /**
     * Tests that a discovery response dumped by the {@link BinaryDiscoveryDumper} is correctly
     * deserialized and stored in the source folder.
     *
     * @throws InterruptedException if an error occurs
     * @throws IOException if an I/O error occurs
     */
    @Test
    public void testDeserialize() throws IOException, InterruptedException {
        File targetDir = new File(tmpFolder.newFolder("discovery-dumps"), "text");
        DiscoveryResponse response = dumpDiscovery(TGT_ID, DiscoveryType.FULL);
        Deserializer.deserialize(dumpDir.getAbsolutePath(), targetDir.getAbsolutePath());
        Assert.assertEquals(targetDir.listFiles().length, 1);
        for (File readableResponse :targetDir.listFiles()) {
            String txtResponse = new String(Files.readAllBytes(readableResponse.toPath()));
            Assert.assertEquals(response.toString(), txtResponse);
        }
    }

    private DiscoveryResponse dumpDiscovery(String target, DiscoveryType discoveryType)
        throws InterruptedException {
        DiscoveryResponse response = DiscoveryResponse.newBuilder().addErrorDTO(
            ErrorDTO.newBuilder()
                .setDescription("").setSeverity(ErrorDTO.ErrorSeverity.WARNING).build())
            .build();
        dumper.dumpDiscovery(
            target,
            discoveryType,
            response,
            Collections.emptyList());
        return response;
    }
}
