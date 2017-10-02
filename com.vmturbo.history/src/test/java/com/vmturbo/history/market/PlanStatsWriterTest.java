package com.vmturbo.history.market;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.sql.Connection;

import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.stats.PlanStatsWriter;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.reports.db.abstraction.tables.records.ScenariosRecord;

/**
 * Unit tests for writing plan output stats.
 **/
public class PlanStatsWriterTest {

    @Mock
    HistorydbIO mockHistorydbIO;
    @Mock
    Connection mockSqlConnection;
    @Mock
    ScenariosRecord mockScenariosRecord;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testPersistPriceIndexInfo() throws Exception {
        // Arrange
        when(mockHistorydbIO.connection()).thenReturn(mockSqlConnection);
        when(mockHistorydbIO.getOrAddScenariosRecord(anyLong(), anyLong(), anyLong())).thenReturn(mockScenariosRecord);
        DSLContext testDSLContext = DSL.using(SQLDialect.MYSQL);
        HistorydbIO.setSharedInstance(mockHistorydbIO);
        when(mockHistorydbIO.getJooqBuilder()).thenReturn(testDSLContext);
        when(mockHistorydbIO.execute(any(Query.class), any(Connection.class))).thenReturn(null);
        PlanStatsWriter planStatsWriter = new PlanStatsWriter(mockHistorydbIO);
        PriceIndexMessage priceIndexMessage = PriceIndexMessage.newBuilder()
                .addPayload(PriceIndexDTOs.PriceIndexMessagePayload.newBuilder()
                        .setOid(1).setPriceindexCurrent(10).setPriceindexProjected(100)
                        .build())
                .addPayload(PriceIndexDTOs.PriceIndexMessagePayload.newBuilder()
                        .setOid(2).setPriceindexCurrent(20).setPriceindexProjected(200)
                        .build())
                .addPayload(PriceIndexDTOs.PriceIndexMessagePayload.newBuilder()
                        .setOid(3).setPriceindexCurrent(30).setPriceindexProjected(300)
                        .build())
                .build();
        // Act
        planStatsWriter.persistPlanPriceIndexInfo(priceIndexMessage);
        // Assert
        verify(mockHistorydbIO).connection();
        verify(mockHistorydbIO).getOrAddScenariosRecord(anyLong(), anyLong(), anyLong());
        verify(mockHistorydbIO, times(2)).JooqBuilder();

        // two execute(), one each for current and projected
        verify(mockHistorydbIO, times(2)).execute(any(Query.class), any(Connection.class));
        verifyNoMoreInteractions(mockHistorydbIO);
    }

    @Test
    public void testPriceIndexInfoEmpty() {
        // Arrange
        PlanStatsWriter planStatsWriter = new PlanStatsWriter(mockHistorydbIO);
        PriceIndexMessage priceIndexMessage = PriceIndexMessage.newBuilder()
                .build();
        // Act
        planStatsWriter.persistPlanPriceIndexInfo(priceIndexMessage);
        // Assert
        Mockito.verifyZeroInteractions(mockHistorydbIO);
    }
}
