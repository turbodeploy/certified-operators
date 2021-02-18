package com.vmturbo.extractor.grafana;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.sql.Timestamp;
import java.util.Optional;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.extractor.ExtractorGlobalConfig.ExtractorFeatureFlags;
import com.vmturbo.extractor.grafana.Grafanon.GrafanonConfig;
import com.vmturbo.extractor.grafana.client.GrafanaClient;
import com.vmturbo.extractor.grafana.model.FolderInput;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Test Grafanon.
 */
public class GrafanonTest {

    private Grafanon grafanon;
    private DbEndpoint dbendpointMock;
    private DSLContext dslContextSpy;

    /**
     * Test setup.
     */
    @Before
    public void setup() {
        try {
            dbendpointMock = mock(DbEndpoint.class);
            dslContextSpy = spy(DSL.using(SQLDialect.POSTGRES));
            doReturn(dslContextSpy).when(dbendpointMock).dslContext();

            GrafanonConfig grafanonConfigMock = mock(GrafanonConfig.class);
            DashboardsOnDisk dashboardsOnDiskMock = mock(DashboardsOnDisk.class);
            GrafanaClient grafanaClientMock = mock(GrafanaClient.class);
            ExtractorFeatureFlags extractorFeatureFlags = mock(ExtractorFeatureFlags.class);

            grafanon = new Grafanon(grafanonConfigMock, dashboardsOnDiskMock, grafanaClientMock, extractorFeatureFlags, dbendpointMock);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Test timestamp stringified from results.
     * @throws Exception something went wrong
     */
    @Test
    public void testGetMigrationV14TimeStamp() throws Exception {
        //GIVEN
        long time = 222L;
        Timestamp timestamp = new Timestamp(time);

        Field field = DSL.field("installed_on");
        Result<Record1> result = dslContextSpy.newResult(field);
        result.add(dslContextSpy.newRecord(field).values(timestamp));
        doReturn(result).when(dslContextSpy).fetch(any(String.class));

        //WHEN
        String migrationTimestamp = grafanon.getMigrationV14TimeStamp();

        //THEN
        assertEquals("Dec 31, 1969 7:00:00 PM", migrationTimestamp);
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
