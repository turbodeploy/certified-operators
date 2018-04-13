package com.vmturbo.plan.orchestrator.diagnostics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipOutputStream;

import org.junit.Test;

import io.prometheus.client.CollectorRegistry;

import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.Diagnosable.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.RecursiveZipReader;
import com.vmturbo.components.common.diagnostics.RecursiveZipReaderFactory;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileDaoImpl;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.project.PlanProjectDao;
import com.vmturbo.plan.orchestrator.reservation.ReservationDao;
import com.vmturbo.plan.orchestrator.scenario.ScenarioDao;
import com.vmturbo.plan.orchestrator.templates.TemplateSpecParser;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;

public class PlanOrchestratorDiagnosticsHandlerTest {

    private static final List<String> SUCCESS_MSG =
        Collections.singletonList("diags have been collected!");
    private static final String ERROR_MSG = "This is bad and you should feel bad.";

    private final PlanDao mockPlanDao = mock(PlanDao.class);
    private final PlanProjectDao mockPlanProjectDao = mock(PlanProjectDao.class);
    private final ReservationDao mockReservationDao = mock(ReservationDao.class);
    private final ScenarioDao mockScenarioDao = mock(ScenarioDao.class);
    private final TemplatesDao mockTemplateDao = mock(TemplatesDao.class);
    private final TemplateSpecParser mockTemplateSpecParser = mock(TemplateSpecParser.class);
    private final DeploymentProfileDaoImpl mockDeploymentProfileDao =
        mock(DeploymentProfileDaoImpl.class);

    private final List<Diagnosable> diagMocks = Arrays.asList(mockDeploymentProfileDao,
        mockTemplateSpecParser, mockTemplateDao, mockScenarioDao, mockReservationDao,
        mockPlanProjectDao, mockPlanDao);

    private final RecursiveZipReaderFactory mockReaderFactory =
        mock(RecursiveZipReaderFactory.class);
    private final DiagnosticsWriter mockWriter = mock(DiagnosticsWriter.class);

    @Test
    public void testDump() throws DiagnosticsException {
        for (Diagnosable diagMock : diagMocks) {
            when(diagMock.collectDiags())
                .thenReturn(SUCCESS_MSG);
        }

        final PlanOrchestratorDiagnosticsHandler handler =
            new PlanOrchestratorDiagnosticsHandler(mockPlanDao, mockPlanProjectDao,
                mockReservationDao, mockScenarioDao, mockTemplateDao,
                mockTemplateSpecParser, mockDeploymentProfileDao, mockReaderFactory, mockWriter);

        final ZipOutputStream mockZos = mock(ZipOutputStream.class);

        assertTrue(handler.dump(mockZos).isEmpty());

        for (Diagnosable diagMock : diagMocks) {
            final String filename = handler.filenameToDiagnosableMap.inverse().get(diagMock);
            verify(mockWriter).writeZipEntry(filename, SUCCESS_MSG, mockZos);
        }
        verify(mockWriter, never())
            .writeZipEntry(eq(PlanOrchestratorDiagnosticsHandler.ERRORS_FILE),
                anyListOf(String.class), any());
        verify(mockWriter).writePrometheusMetrics(any(CollectorRegistry.class), eq(mockZos));
    }

    @Test
    public void testDumpFail() throws DiagnosticsException {
        for (Diagnosable diagMock : diagMocks) {
            when(diagMock.collectDiags())
                .thenThrow(new DiagnosticsException(Collections.singletonList(ERROR_MSG)));
        }

        final PlanOrchestratorDiagnosticsHandler handler =
            new PlanOrchestratorDiagnosticsHandler(mockPlanDao, mockPlanProjectDao,
                mockReservationDao, mockScenarioDao, mockTemplateDao,
                mockTemplateSpecParser, mockDeploymentProfileDao, mockReaderFactory, mockWriter);

        final ZipOutputStream mockZos = mock(ZipOutputStream.class);

        final List<String> errors = handler.dump(mockZos);

        assertEquals(diagMocks.size(), errors.size());
        assertTrue(errors.stream().allMatch(error -> error.equals(ERROR_MSG)));

        for (Diagnosable diagMock : diagMocks) {
            final String filename = handler.filenameToDiagnosableMap.inverse().get(diagMock);
            verify(mockWriter, never()).writeZipEntry(eq(filename), anyListOf(String.class), any());
        }
        verify(mockWriter).writeZipEntry(eq(PlanOrchestratorDiagnosticsHandler.ERRORS_FILE),
                eq(errors), eq(mockZos));
    }

    @Test
    public void testRestore() throws DiagnosticsException {

        final PlanOrchestratorDiagnosticsHandler handler =
            new PlanOrchestratorDiagnosticsHandler(mockPlanDao, mockPlanProjectDao,
                mockReservationDao, mockScenarioDao, mockTemplateDao,
                mockTemplateSpecParser, mockDeploymentProfileDao, mockReaderFactory, mockWriter);

        final List<Diags> diagsList = new ArrayList<>();

        for (Diagnosable diagMock : diagMocks) {
            final Diags diags = mock(Diags.class);
            when(diags.getName())
                .thenReturn(handler.filenameToDiagnosableMap.inverse().get(diagMock));
            when(diags.getLines()).thenReturn(SUCCESS_MSG);
            diagsList.add(diags);
        }

        final InputStream mockInputStream = mock(InputStream.class);
        final RecursiveZipReader mockReader = mock(RecursiveZipReader.class);
        when(mockReaderFactory.createReader(mockInputStream)).thenReturn(mockReader);
        when(mockReader.iterator()).thenReturn(diagsList.iterator());

        assertTrue(handler.restore(mockInputStream).isEmpty());

        for (Diagnosable diagMock : diagMocks) {
            verify(diagMock).restoreDiags(SUCCESS_MSG);
        }
    }

    @Test
    public void testRestoreEmpty() throws DiagnosticsException {

        final PlanOrchestratorDiagnosticsHandler handler =
            new PlanOrchestratorDiagnosticsHandler(mockPlanDao, mockPlanProjectDao,
                mockReservationDao, mockScenarioDao, mockTemplateDao,
                mockTemplateSpecParser, mockDeploymentProfileDao, mockReaderFactory, mockWriter);

        final List<Diags> diagsList = new ArrayList<>();

        for (Diagnosable diagMock : diagMocks) {
            final Diags diags = mock(Diags.class);
            when(diags.getName())
                .thenReturn(handler.filenameToDiagnosableMap.inverse().get(diagMock));
            when(diags.getLines()).thenReturn(null);
            diagsList.add(diags);
        }

        final InputStream mockInputStream = mock(InputStream.class);
        final RecursiveZipReader mockReader = mock(RecursiveZipReader.class);
        when(mockReaderFactory.createReader(mockInputStream)).thenReturn(mockReader);
        when(mockReader.iterator()).thenReturn(diagsList.iterator());

        final List<String> errors = handler.restore(mockInputStream);

        assertEquals(diagMocks.size(), errors.size());
        assertTrue(errors.stream().allMatch(string -> string.startsWith("Unable to restore")));

        for (Diagnosable diagMock : diagMocks) {
            verify(diagMock, never()).restoreDiags(anyListOf(String.class));
        }
    }


    @Test
    public void testRestoreFail() throws DiagnosticsException {

        final PlanOrchestratorDiagnosticsHandler handler =
            new PlanOrchestratorDiagnosticsHandler(mockPlanDao, mockPlanProjectDao,
                mockReservationDao, mockScenarioDao, mockTemplateDao,
                mockTemplateSpecParser, mockDeploymentProfileDao, mockReaderFactory, mockWriter);

        final List<Diags> diagsList = new ArrayList<>();

        for (Diagnosable diagMock : diagMocks) {
            final Diags diags = mock(Diags.class);
            when(diags.getName())
                .thenReturn(handler.filenameToDiagnosableMap.inverse().get(diagMock));
            doThrow(new DiagnosticsException(Collections.singletonList(ERROR_MSG)))
                .when(diagMock).restoreDiags(anyListOf(String.class));
            diagsList.add(diags);
        }

        final InputStream mockInputStream = mock(InputStream.class);
        final RecursiveZipReader mockReader = mock(RecursiveZipReader.class);
        when(mockReaderFactory.createReader(mockInputStream)).thenReturn(mockReader);
        when(mockReader.iterator()).thenReturn(diagsList.iterator());

        final List<String> errors = handler.restore(mockInputStream);

        assertEquals(diagMocks.size(), errors.size());
        assertTrue(errors.stream().allMatch(string -> string.equals(ERROR_MSG)));
    }

}
