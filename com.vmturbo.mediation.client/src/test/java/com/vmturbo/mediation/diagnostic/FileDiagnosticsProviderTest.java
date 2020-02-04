package com.vmturbo.mediation.diagnostic;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.components.common.diagnostics.DiagnosticsException;

/**
 * Unit test for {@link FileDiagnosticsProvider}.
 */
public class FileDiagnosticsProviderTest {

    /**
     * Temporary folder to store files.
     */
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();
    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Tests directory passed to constructor of a provider. Exception is expected.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testDirectoryPassed() throws Exception {
        final File directory = tmpFolder.newFolder();
        final FileDiagnosticsProvider provider = new FileDiagnosticsProvider(directory);
        final OutputStream os = new ByteArrayOutputStream();
        expectedException.expect(DiagnosticsException.class);
        expectedException.expectMessage("File requested to dump is not a regular file");
        provider.collectDiags(os);
    }
}
