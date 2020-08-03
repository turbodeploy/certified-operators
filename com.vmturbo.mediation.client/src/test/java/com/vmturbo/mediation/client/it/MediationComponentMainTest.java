package com.vmturbo.mediation.client.it;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.springframework.test.util.ReflectionTestUtils;

import com.vmturbo.mediation.client.MediationComponentMain;

/**
 * Unit test for testing {@link MediationComponentMain}
 */
public class MediationComponentMainTest extends MediationComponentMain {

    private static String ZIP_STREAM_ENTRIES_FIELD = "names";
    private static String TMP_AWS_BILLING_PATH = "/tmp/diags/aws/billing/";
    private static String MOCK_DIAGS_FILE = "mockBillingDiags";

    /**
     * Creating a mock diags file that will get checked later.
     *
     * @throws IOException if creating the directories/files fail.
     */
    @Before
    public final void init() throws IOException {
        ReflectionTestUtils.setField(this, "diagnosticsConfig", new MediationDiagnosticsConfigTest());
        String fullDiagsPath = TMP_AWS_BILLING_PATH + MOCK_DIAGS_FILE;
        createDir(fullDiagsPath);
        createFile(fullDiagsPath);
    }

    /**
     * Mocking a dumpDiags call and checking that the mocked diags file is being added to
     * the ZipOutputStream.
     */
    @Ignore
    @Test
    public void testOnDumpDiags() {
        ZipOutputStream zos = new ZipOutputStream(new OutputStream() {
            @Override
            public void write(final int b) {
            }
        });
        onDumpDiags(zos);
        Set zipEntries = (Set<String>)Whitebox.getInternalState(zos, ZIP_STREAM_ENTRIES_FIELD);
        Assert.assertTrue(zipEntries.contains(TMP_AWS_BILLING_PATH + MOCK_DIAGS_FILE));
    }

    private void createFile(String fileName) throws IOException {
        File newFile = new File(fileName);
        newFile.createNewFile();
        newFile.deleteOnExit();
    }

    private void createDir(final String dumpDiagsTmpDir) {
        File file = new File(dumpDiagsTmpDir);
        file.getParentFile().mkdirs();
    }
}

