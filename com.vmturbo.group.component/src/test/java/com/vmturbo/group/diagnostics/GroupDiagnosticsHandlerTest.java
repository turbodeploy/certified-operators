package com.vmturbo.group.diagnostics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import io.prometheus.client.CollectorRegistry;

import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.Diagnosable.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.RecursiveZipReader;
import com.vmturbo.components.common.diagnostics.RecursiveZipReaderFactory;
import com.vmturbo.group.persistent.GroupStore;
import com.vmturbo.group.persistent.PolicyStore;
import com.vmturbo.group.persistent.SettingStore;

/**
 * Tests for {@link GroupDiagnosticsHandler}.
 */
@RunWith(MockitoJUnitRunner.class)
public class GroupDiagnosticsHandlerTest {
    private RecursiveZipReaderFactory zipReaderFactory = mock(RecursiveZipReaderFactory.class);

    private DiagnosticsWriter diagnosticsWriter = mock(DiagnosticsWriter.class);

    private GroupStore groupStore;

    private PolicyStore policyStore;

    private SettingStore settingStore;

    private final List<String> groupLines = Collections.singletonList("some groups");
    private final List<String> policyLines = Collections.singletonList("some policies");
    private final List<String> settingLines = Collections.singletonList("some settings");

    @Before
    public void setUp() throws Exception {
        groupStore = mock(GroupStore.class);
        policyStore = mock(PolicyStore.class);
        settingStore = mock(SettingStore.class);
    }

    @Test
    public void testDump() throws DiagnosticsException {
        setupDump();

        final GroupDiagnosticsHandler handler =
            new GroupDiagnosticsHandler(groupStore, policyStore, settingStore,
                zipReaderFactory, diagnosticsWriter);
        final ZipOutputStream zos = mock(ZipOutputStream.class);
        handler.dump(zos);

        verify(diagnosticsWriter).writeZipEntry(eq(GroupDiagnosticsHandler.GROUPS_DUMP_FILE),
            eq(groupLines), eq(zos));
        verify(diagnosticsWriter).writeZipEntry(eq(GroupDiagnosticsHandler.POLICIES_DUMP_FILE),
            eq(policyLines), eq(zos));
        verify(diagnosticsWriter).writeZipEntry(eq(GroupDiagnosticsHandler.SETTINGS_DUMP_FILE),
            eq(settingLines), eq(zos));
        verify(diagnosticsWriter).writePrometheusMetrics(any(CollectorRegistry.class),
            eq(zos));
        verify(diagnosticsWriter, times(0)).writeZipEntry(
            eq(GroupDiagnosticsHandler.ERRORS_FILE), anyList(), any());
    }

    @Test
    public void testDumpException() throws DiagnosticsException {
        setupDump();

        when(groupStore.collectDiags())
            .thenThrow(new DiagnosticsException(Collections.singletonList("GROUP ERRORS")));
        when(policyStore.collectDiags())
            .thenThrow(new DiagnosticsException(Collections.singletonList("POLICY ERRORS")));

        final GroupDiagnosticsHandler handler =
            new GroupDiagnosticsHandler(groupStore, policyStore, settingStore,
                zipReaderFactory, diagnosticsWriter);
        final ZipOutputStream zos = mock(ZipOutputStream.class);
        final List<String> errors = handler.dump(zos);

        assertThat(errors, containsInAnyOrder("GROUP ERRORS", "POLICY ERRORS"));

        // Group Dump file shouldn't get written.
        verify(diagnosticsWriter, never())
            .writeZipEntry(eq(GroupDiagnosticsHandler.GROUPS_DUMP_FILE), anyList(), eq(zos));

        // Policy Dump file shouldn't get written.
        verify(diagnosticsWriter, never())
            .writeZipEntry(eq(GroupDiagnosticsHandler.GROUPS_DUMP_FILE), anyList(), eq(zos));

        // Setting Dump file SHOULD get written because it does not generate errors.
        verify(diagnosticsWriter).writeZipEntry(eq(GroupDiagnosticsHandler.SETTINGS_DUMP_FILE),
            eq(settingLines), eq(zos));

        verify(diagnosticsWriter).writeZipEntry(
            eq(GroupDiagnosticsHandler.ERRORS_FILE), eq(errors), eq(zos));
    }

    @Test
    public void testRestore() throws DiagnosticsException {
        final Diags groupDiags = mock(Diags.class);
        when(groupDiags.getName()).thenReturn(GroupDiagnosticsHandler.GROUPS_DUMP_FILE);
        when(groupDiags.getLines()).thenReturn(groupLines);

        final Diags policyDiags = mock(Diags.class);
        when(policyDiags.getName()).thenReturn(GroupDiagnosticsHandler.POLICIES_DUMP_FILE);
        when(policyDiags.getLines()).thenReturn(policyLines);

        final Diags settingDiags = mock(Diags.class);
        when(settingDiags.getName()).thenReturn(GroupDiagnosticsHandler.SETTINGS_DUMP_FILE);
        when(settingDiags.getLines()).thenReturn(settingLines);

        setupRestore(groupDiags, policyDiags, settingDiags);

        final GroupDiagnosticsHandler handler =
            new GroupDiagnosticsHandler(groupStore, policyStore, settingStore,
                zipReaderFactory, diagnosticsWriter);
        List<String> errors = handler.restore(mock(ZipInputStream.class));
        assertTrue(errors.isEmpty());

        verify(groupStore).restoreDiags(eq(groupLines));
        verify(policyStore).restoreDiags(eq(policyLines));
        verify(settingStore).restoreDiags(eq(settingLines));
    }

    /**
     * Sets up the mocks for various calls required for the
     * {@link GroupDiagnosticsHandler#dump(ZipOutputStream)} method to work.
     * Tests that want to mess with certain subsets of the mocks (e.g. make the rest template
     * throw an exception) can do that setup after calling this method.
     */
    private void setupDump() throws DiagnosticsException {
        doReturn(groupLines).when(groupStore).collectDiags();
        doReturn(policyLines).when(policyStore).collectDiags();
        doReturn(settingLines).when(settingStore).collectDiags();
    }

    private void setupRestore(Diags diags, Diags... otherDiags) {
        final RecursiveZipReader zipReader = mock(RecursiveZipReader.class);
        final Iterator<Diags> diagsIt = (Iterator<Diags>)mock(Iterator.class);

        Boolean[] bools = new Boolean[otherDiags.length + 1];
        Arrays.fill(bools, true);
        if (bools.length > 0) {
            bools[bools.length - 1] = false;
        }

        when(diagsIt.hasNext()).thenReturn(true, bools);
        when(diagsIt.next()).thenReturn(diags, otherDiags);

        when(zipReader.iterator()).thenReturn(diagsIt);
        when(zipReaderFactory.createReader(any(InputStream.class))).thenReturn(zipReader);
    }
}