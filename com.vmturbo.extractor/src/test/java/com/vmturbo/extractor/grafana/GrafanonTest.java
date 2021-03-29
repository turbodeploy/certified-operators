package com.vmturbo.extractor.grafana;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;

import reactor.core.publisher.Flux;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.LicenseProtoUtil;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.extractor.ExtractorGlobalConfig.ExtractorFeatureFlags;
import com.vmturbo.extractor.grafana.Grafanon.GrafanonConfig;
import com.vmturbo.extractor.grafana.client.GrafanaClient;
import com.vmturbo.extractor.grafana.model.FolderInput;
import com.vmturbo.extractor.grafana.model.Role;
import com.vmturbo.extractor.grafana.model.UserInput;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Test Grafanon.
 */
public class GrafanonTest {

    private Grafanon grafanon;
    private DbEndpoint dbendpointMock;
    private DSLContext dslContextSpy;

    private GrafanaClient grafanaClientMock = mock(GrafanaClient.class);
    private GrafanonConfig grafanonConfigMock = mock(GrafanonConfig.class);
    private LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);

    private final int reportEditorCount = 2;

    private DashboardsOnDisk dashboardsOnDiskMock = mock(DashboardsOnDisk.class);
    private ExtractorFeatureFlags extractorFeatureFlags = mock(ExtractorFeatureFlags.class);

    /**
     * Test setup.
     *
     * @throws Exception If anything is wrong.
     */
    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        dbendpointMock = mock(DbEndpoint.class);
        dslContextSpy = spy(DSL.using(SQLDialect.POSTGRES));
        doReturn(dslContextSpy).when(dbendpointMock).dslContext();


        Flux<LicenseSummary> fakeStream = Flux.empty();
        when(licenseCheckClient.getUpdateEventStream()).thenReturn(fakeStream);
        grafanon = new Grafanon(grafanonConfigMock, dashboardsOnDiskMock, grafanaClientMock, extractorFeatureFlags, dbendpointMock, licenseCheckClient);
    }

    /**
     * Test that refreshing turbo editors is triggered by a license summary update,
     * and makes the necessary calls to the Grafana client.
     */
    @Test
    public void testRefreshEditors() {
        // ARRANGE
        final String editorPrefix = "turbo-report-editor";
        final String editorUsername = "Me";
        when(grafanonConfigMock.getEditorUsernamePrefix()).thenReturn(editorPrefix);
        when(grafanonConfigMock.getEditorDisplayName()).thenReturn(editorUsername);
        when(grafanonConfigMock.validEditorConfig()).thenReturn(true);
        Flux<LicenseSummary> streamWithLicense = Flux.just(LicenseSummary.newBuilder()
                .setMaxReportEditorsCount(reportEditorCount)
                .build());
        when(licenseCheckClient.getUpdateEventStream()).thenReturn(streamWithLicense);

        verifyZeroInteractions(grafanaClientMock);

        // ACT
        new Grafanon(grafanonConfigMock, dashboardsOnDiskMock, grafanaClientMock, extractorFeatureFlags, dbendpointMock, licenseCheckClient);

        // ASSERT
        ArgumentCaptor<UserInput> inputCaptor = ArgumentCaptor.forClass(UserInput.class);
        verify(grafanaClientMock, times(reportEditorCount)).ensureUserExists(inputCaptor.capture(), eq(Role.ADMIN), any());
        List<UserInput> capturedInputs = inputCaptor.getAllValues();
        Set<String> expectedNames = IntStream.range(0, reportEditorCount)
                .mapToObj(idx -> LicenseProtoUtil.formatReportEditorUsername(editorPrefix, idx))
                .collect(Collectors.toSet());
        assertThat(capturedInputs.stream()
                .map(UserInput::getUsername).collect(Collectors.toSet()), is(expectedNames));
    }

    /**
     * Test timestamp stringified from results.
     * @throws Exception something went wrong
     */
    @Test
    public void testGetMigrationV14TimeStamp() throws Exception {
        //GIVEN
        long time = 99999000L;
        Timestamp timestamp = new Timestamp(time);

        Field field = DSL.field("installed_on");
        Result<Record1> result = dslContextSpy.newResult(field);
        result.add(dslContextSpy.newRecord(field).values(timestamp));
        doReturn(result).when(dslContextSpy).fetch(any(String.class));

        //WHEN
        String migrationTimestamp = grafanon.getMigrationV14TimeStamp();

        //THEN
        assertEquals(DateFormat.getDateTimeInstance().parse(migrationTimestamp).getTime(), time);
    }

    /**
     * Test no results results in empty string.
     */
    @Test
    public void testGetMigrationV14TimeStampFindsNoResult() {
        //GIVEN
        Field field = DSL.field("installed_on");
        Result<Record1> result = dslContextSpy.newResult(field);
        doReturn(result).when(dslContextSpy).fetch(any(String.class));

        //WHEN
        String migrationTimestamp = grafanon.getMigrationV14TimeStamp();

        //THEN
        assertEquals("", migrationTimestamp);
    }

    /**
     * Test timestamp is added to reportV1.
     */
    @Test
    public void testAddTimestampToReportsV1FolderTitle() {
        //GIVEN
        FolderInput folderInput = new FolderInput();
        folderInput.setTitle("title %s");
        folderInput.setUid(Grafanon.REPORTS_V1_FOLDER_UUID);
        String migrationTimestamp = "timestamp123";

        //WHEN
        grafanon.addTimestampToReportsV1FolderTitle(folderInput, migrationTimestamp);

        //THEN
        assertTrue(folderInput.getTitle().contains(migrationTimestamp));
    }

    /**
     * Test skipFolder return true for reports_v1 if entity_old has no data.
     */
    @Test
    public void testSkipFolder() {
        //GIVEN
        FolderInput folderInput = new FolderInput();
        folderInput.setTitle("title %s");
        folderInput.setUid(Grafanon.REPORTS_V1_FOLDER_UUID);

        //Mock returning empty result
        Field field = DSL.field("installed_on");
        Result<Record1> result = dslContextSpy.newResult(field);
        doReturn(result).when(dslContextSpy).fetch(any(String.class));

        //THEN
        assertTrue(grafanon.skipFolder(Optional.of(folderInput)));
    }

    /**
     * Test skipFolder return false for reports_v1 if entity_old has data.
     */
    @Test
    public void testSkipFolderEntityOldHasData() {
        //GIVEN
        FolderInput folderInput = new FolderInput();
        folderInput.setTitle("title %s");
        folderInput.setUid(Grafanon.REPORTS_V1_FOLDER_UUID);

        //Mock returning empty result
        Field field = DSL.field("installed_on");
        Result<Record1> result = dslContextSpy.newResult(field);
        result.add(dslContextSpy.newRecord(field).values(""));
        doReturn(result).when(dslContextSpy).fetch(any(String.class));

        //THEN
        assertFalse(grafanon.skipFolder(Optional.of(folderInput)));
    }

    /**
     * Test skipFolder return false for non reports_v1.
     */
    @Test
    public void testSkipFolderForRandomFolder() {
        //GIVEN
        FolderInput folderInput = new FolderInput();
        folderInput.setTitle("title %s");
        folderInput.setUid("RANDOM");

        //THEN
        assertFalse(grafanon.skipFolder(Optional.of(folderInput)));
    }

    /**
     * Test skipFolder return false for emtpy folderInput.
     */
    @Test
    public void testSkipFolderForEmptyFolder() {
        assertFalse(grafanon.skipFolder(Optional.empty()));
    }

}
