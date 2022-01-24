package com.vmturbo.market.diagnostics;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.analysis.economy.Economy;

/**
 * Test for AnalysisDiagnosticsCleaner.
 */
public class AnalysisDiagnosticsCleanerTest {

    private IDiagsFileSystem mockFileSystem;
    private Economy economy;
    private TopologyInfo rtTopoInfo;
    private TopologyInfo planTopoInfo;
    private String rtExpectedFullPath;
    private String planExpectedFullPath;

    /**
     * Setup an economy, real time and plan TopologyInfos.
     */
    @Before
    public void setup() {
        mockFileSystem = mock(IDiagsFileSystem.class);
        economy = new Economy();
        rtTopoInfo = TopologyInfo.newBuilder().setTopologyContextId(777777L).setTopologyId(1L).build();
        planTopoInfo = TopologyInfo.newBuilder().setTopologyContextId(1234L).setTopologyId(2L).setPlanInfo(
                TopologyDTO.PlanTopologyInfo.getDefaultInstance()).build();
        rtExpectedFullPath = AnalysisDiagnosticsUtils.getZipFileFullPath(
                AnalysisDiagnosticsUtils.getFilePrefix(AnalysisDiagnosticsCollector.AnalysisMode.M2),
                AnalysisDiagnosticsUtils.getTopologyUniqueIdentifier(rtTopoInfo));
        planExpectedFullPath = AnalysisDiagnosticsUtils.getZipFileFullPath(
                AnalysisDiagnosticsUtils.getFilePrefix(AnalysisDiagnosticsCollector.AnalysisMode.M2),
                AnalysisDiagnosticsUtils.getTopologyUniqueIdentifier(planTopoInfo));
        Configurator.setLevel(LogManager.getLogger(AnalysisDiagnosticsCollector.class).getName(), Level.INFO);
    }

    /**
     * For real time with exceptions, diags should not be deleted.
     */
    @Test
    public void testDiagsNotDeletedForRTWithExceptions() {
        when(mockFileSystem.listFiles(any())).thenReturn(Stream.of(Paths.get(rtExpectedFullPath)));
        IDiagnosticsCleaner cleaner = new AnalysisDiagnosticsCleaner(10, 3, mockFileSystem);
        cleaner.setSaveAnalysisDiagsFuture(CompletableFuture.completedFuture(null));
        economy.getExceptionTraders().add(1L);

        cleaner.cleanup(economy, rtTopoInfo);

        verify(mockFileSystem, times(0)).deleteIfExists(Paths.get(rtExpectedFullPath));
    }

    /**
     * For real time with exceptions, diags should not be deleted.
     */
    @Test
    public void testDiagsDeletedForRTWithoutExceptions() {
        when(mockFileSystem.listFiles(any())).thenReturn(Stream.empty());
        IDiagnosticsCleaner cleaner = new AnalysisDiagnosticsCleaner(10, 3, mockFileSystem);
        cleaner.setSaveAnalysisDiagsFuture(CompletableFuture.completedFuture(null));

        cleaner.cleanup(economy, rtTopoInfo);

        verify(mockFileSystem, times(1)).deleteIfExists(Paths.get(rtExpectedFullPath));
    }

    /**
     * When debug is enabled, diags should not be deleted.
     */
    @Test
    public void testDiagsNotDeletedWhenDebugEnabled() {
        Configurator.setLevel(LogManager.getLogger(AnalysisDiagnosticsCollector.class).getName(), Level.DEBUG);
        when(mockFileSystem.listFiles(any())).thenReturn(Stream.of(Paths.get(rtExpectedFullPath)));

        IDiagnosticsCleaner cleaner = new AnalysisDiagnosticsCleaner(10, 3, mockFileSystem);
        cleaner.setSaveAnalysisDiagsFuture(CompletableFuture.completedFuture(null));
        cleaner.cleanup(economy, rtTopoInfo);

        when(mockFileSystem.listFiles(any())).thenReturn(Stream.of(Paths.get(rtExpectedFullPath), Paths.get(planExpectedFullPath)));
        cleaner.cleanup(economy, planTopoInfo);

        verify(mockFileSystem, times(0)).deleteIfExists(Paths.get(rtExpectedFullPath));
    }

    /**
     * For plans, diags should not be deleted.
     */
    @Test
    public void testPlanDiagsNotDeleted() {
        when(mockFileSystem.listFiles(any())).thenReturn(Stream.of(Paths.get(planExpectedFullPath), Paths.get(planExpectedFullPath)));
        IDiagnosticsCleaner cleaner = new AnalysisDiagnosticsCleaner(10, 1, mockFileSystem);
        cleaner.setSaveAnalysisDiagsFuture(CompletableFuture.completedFuture(null));

        cleaner.cleanup(economy, rtTopoInfo);

        verify(mockFileSystem, times(1)).deleteIfExists(Paths.get(rtExpectedFullPath));
        verify(mockFileSystem, times(0)).deleteIfExists(Paths.get(planExpectedFullPath));
    }

    /**
     * Number of real time analysis diags to retain is setup as 1.
     * There are already 2 sets of real time diags in the file system. One more set gets saved.
     * When cleanup is called, the 2 old diags should be deleted, leaving only the one which was just saved.
     */
    @Test
    public void testOldDiagsDeleted() {
        TopologyInfo oldRtTopoInfo1 = TopologyInfo.newBuilder().setTopologyContextId(777777L).setTopologyId(2L).build();
        Path oldRtExpectedFullPath1 = Paths.get(AnalysisDiagnosticsUtils.getZipFileFullPath(
                AnalysisDiagnosticsUtils.getFilePrefix(AnalysisDiagnosticsCollector.AnalysisMode.M2),
                AnalysisDiagnosticsUtils.getTopologyUniqueIdentifier(oldRtTopoInfo1)));
        TopologyInfo oldRtTopoInfo2 = TopologyInfo.newBuilder().setTopologyContextId(777777L).setTopologyId(3L).build();
        Path oldRtExpectedFullPath2 = Paths.get(AnalysisDiagnosticsUtils.getZipFileFullPath(
                AnalysisDiagnosticsUtils.getFilePrefix(AnalysisDiagnosticsCollector.AnalysisMode.M2),
                AnalysisDiagnosticsUtils.getTopologyUniqueIdentifier(oldRtTopoInfo2)));

        when(mockFileSystem.listFiles(any())).thenReturn(Stream.of(oldRtExpectedFullPath1, oldRtExpectedFullPath2, Paths.get(rtExpectedFullPath)));
        when(mockFileSystem.isDirectory(any())).thenReturn(false);
        when(mockFileSystem.getLastModified(oldRtExpectedFullPath1)).thenReturn(100L);
        when(mockFileSystem.getLastModified(oldRtExpectedFullPath1)).thenReturn(101L);
        when(mockFileSystem.getLastModified(Paths.get(rtExpectedFullPath))).thenReturn(200L);
        IDiagnosticsCleaner cleaner = new AnalysisDiagnosticsCleaner(10, 1, mockFileSystem);
        cleaner.setSaveAnalysisDiagsFuture(CompletableFuture.completedFuture(null));
        economy.getExceptionTraders().add(1L);

        cleaner.cleanup(economy, rtTopoInfo);

        verify(mockFileSystem, times(1)).deleteIfExists(oldRtExpectedFullPath1);
        verify(mockFileSystem, times(1)).deleteIfExists(oldRtExpectedFullPath2);
        verify(mockFileSystem, times(0)).deleteIfExists(Paths.get(rtExpectedFullPath));
    }

    /**
     * When the feature is disabled, there should no interactions with the file system.
     */
    @Test
    public void testFeatureDisabled() {
        IDiagnosticsCleaner cleaner = new AnalysisDiagnosticsCleaner(10, 0, mockFileSystem);
        cleaner.setSaveAnalysisDiagsFuture(CompletableFuture.completedFuture(null));
        cleaner.cleanup(economy, rtTopoInfo);

        verifyNoMoreInteractions(mockFileSystem);
    }
}