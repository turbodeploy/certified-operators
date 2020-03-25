package com.vmturbo.topology.processor.historical;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.jooq.DSLContext;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.mockito.Mockito;
import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.topology.HistoricalInfo.HistoricalInfoDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.topology.processor.db.TopologyProcessor;
import com.vmturbo.topology.processor.db.tables.HistoricalUtilization;
import com.vmturbo.topology.processor.db.tables.records.HistoricalUtilizationRecord;

public class HistoricalUtilizationDatabaseTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(TopologyProcessor.TOPOLOGY_PROCESSOR);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private static final CommodityType SOLD_COMMODITY_TYPE = CommodityType.newBuilder()
        .setType(1234)
        .setKey("2333")
        .build();

    private static final CommodityType BOUGHT_COMMODITY_TYPE = CommodityType.newBuilder()
        .setType(5678)
        .setKey("666")
        .build();

    private DSLContext dsl = dbConfig.getDslContext();

    private HistoricalCommodityInfo soldCommInfo = new HistoricalCommodityInfo();
    private HistoricalCommodityInfo boughtCommInfo = new HistoricalCommodityInfo();

    private HistoricalServiceEntityInfo createHistoricalSeInfo() {
        // Creating HistoricalCommodityInfo for sold commodities
        soldCommInfo.setCommodityTypeAndKey(SOLD_COMMODITY_TYPE);
        soldCommInfo.setHistoricalUsed(10);
        soldCommInfo.setHistoricalPeak(20);
        soldCommInfo.setSourceId(123);
        soldCommInfo.setMatched(true);
        soldCommInfo.setUpdated(false);

        // Creating HistoricalCommodityInfo for bought commodities
        boughtCommInfo.setCommodityTypeAndKey(BOUGHT_COMMODITY_TYPE);
        boughtCommInfo.setHistoricalUsed(30);
        boughtCommInfo.setHistoricalPeak(40);
        boughtCommInfo.setSourceId(456);
        boughtCommInfo.setMatched(false);
        boughtCommInfo.setUpdated(true);

        // Creating HistoricalServiceEntityInfo
        HistoricalServiceEntityInfo seInfo = new HistoricalServiceEntityInfo();
        seInfo.setSeOid(12345678);
        seInfo.getHistoricalCommoditySold().add(soldCommInfo);
        seInfo.getHistoricalCommodityBought().add(boughtCommInfo);
        return seInfo;
    }

    // test save and read when the max allowed size for query is smaller than the protobuf to be saved
    @Test
    public void testWriteReadWithDataLargerThanDBMaxAllowedPackage() {
        HistoricalUtilizationDatabase db = Mockito.spy(new HistoricalUtilizationDatabase(dsl));
        HistoricalInfo info = new HistoricalInfo();
        HistoricalServiceEntityInfo seInfo = createHistoricalSeInfo();
        info.put(0L, seInfo);
        Mockito.when(db.queryMaxAllowedPackageSize()).thenReturn(16);
        Mockito.when(db.shouldPersistData(Mockito.any())).thenReturn(true);
        db.saveInfo(info);
        List<HistoricalUtilizationRecord> result = dsl.selectFrom(HistoricalUtilization
            .HISTORICAL_UTILIZATION).fetch();
        // the blob size is about 45, so we need 45/(16/4) chunks
        assertTrue(result.size() == 12);
        // Reading the BLOB
        byte[] bytes = db.getInfo();
        HistoricalInfo resultInfo = null;
        if (bytes != null) {
            HistoricalInfoDTO histInfo = null;
            try {
                histInfo = HistoricalInfoDTO.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                //logger.error(e.getMessage());
            }
            resultInfo = Conversions.convertFromDto(histInfo);
        }
        // Assertions
        checkLoadedResult(resultInfo);
    }

    // test save and read when the max allowed size for query is larger than the protobuf to be saved
    @Test
    public void testWriteReadWithDataSmallerThanDBMaxAllowedPackage() throws DbException {
        HistoricalUtilizationDatabase db = Mockito.spy(new HistoricalUtilizationDatabase(dsl));
        HistoricalInfo info = new HistoricalInfo();
        HistoricalServiceEntityInfo seInfo = createHistoricalSeInfo();
        info.put(0L, seInfo);
        Mockito.when(db.queryMaxAllowedPackageSize()).thenReturn(2000000);
        Mockito.when(db.shouldPersistData(Mockito.any())).thenReturn(true);

        // Saving the BLOB
        db.saveInfo(info);
        List<HistoricalUtilizationRecord> result = dsl.selectFrom(HistoricalUtilization
            .HISTORICAL_UTILIZATION).fetch();
        assertTrue(result.size() == 1); // only one row

        // Reading the BLOB
        byte[] bytes = db.getInfo();
        HistoricalInfo resultInfo = null;
        if (bytes != null) {
            HistoricalInfoDTO histInfo = null;
            try {
                histInfo = HistoricalInfoDTO.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                //logger.error(e.getMessage());
            }
            resultInfo = Conversions.convertFromDto(histInfo);
        }

        // Assertions
        checkLoadedResult(resultInfo);
    }

    private void checkLoadedResult(HistoricalInfo resultInfo) {
        HistoricalServiceEntityInfo resultSeInfo = resultInfo.get(12345678);
        assertNotNull(resultSeInfo);
        assertEquals(12345678, resultSeInfo.getSeOid());

        HistoricalCommodityInfo resultSoldCommInfo = resultSeInfo.getHistoricalCommoditySold().get(0);
        assertNotNull(resultSoldCommInfo);
        assertEquals(SOLD_COMMODITY_TYPE, soldCommInfo.getCommodityTypeAndKey());
        assertEquals(10, soldCommInfo.getHistoricalUsed(), 0.00001);
        assertEquals(20, soldCommInfo.getHistoricalPeak(), 0.00001);
        assertEquals(123, soldCommInfo.getSourceId());
        assertTrue(soldCommInfo.getMatched());
        assertFalse(soldCommInfo.getUpdated());

        HistoricalCommodityInfo resultBoughtCommInfo = resultSeInfo.getHistoricalCommodityBought().get(0);
        assertNotNull(resultBoughtCommInfo);
        assertEquals(BOUGHT_COMMODITY_TYPE, boughtCommInfo.getCommodityTypeAndKey());
        assertEquals(30, boughtCommInfo.getHistoricalUsed(), 0.00001);
        assertEquals(40, boughtCommInfo.getHistoricalPeak(), 0.00001);
        assertEquals(456, boughtCommInfo.getSourceId());
        assertFalse(boughtCommInfo.getMatched());
        assertTrue(boughtCommInfo.getUpdated());
    }
}
