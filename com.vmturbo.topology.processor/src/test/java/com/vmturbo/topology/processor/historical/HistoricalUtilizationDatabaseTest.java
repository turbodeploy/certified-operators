package com.vmturbo.topology.processor.historical;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import com.google.protobuf.InvalidProtocolBufferException;
import com.vmturbo.common.protobuf.topology.HistoricalInfo.HistoricalInfoDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=topology_processor"})

public class HistoricalUtilizationDatabaseTest {

    private static final CommodityType SOLD_COMMODITY_TYPE = CommodityType.newBuilder()
        .setType(1234)
        .setKey("2333")
        .build();

    private static final CommodityType BOUGHT_COMMODITY_TYPE = CommodityType.newBuilder()
        .setType(5678)
        .setKey("666")
        .build();

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;
    private DSLContext dsl;

    @Before
    public void setup() {
        dsl = dbConfig.prepareDatabase();
    }

    @After
    public void teardown() {
        dbConfig.clean();
    }

    @Test
    public void testWriteRead() throws DbException {
        HistoricalInfo resultInfo = null;

        // Creating HistoricalCommodityInfo for sold commodities
        HistoricalCommodityInfo soldCommInfo = new HistoricalCommodityInfo();
        soldCommInfo.setCommodityTypeAndKey(SOLD_COMMODITY_TYPE);
        soldCommInfo.setHistoricalUsed(10);
        soldCommInfo.setHistoricalPeak(20);
        soldCommInfo.setSourceId(123);
        soldCommInfo.setMatched(true);
        soldCommInfo.setExisting(false);

        // Creating HistoricalCommodityInfo for bought commodities
        HistoricalCommodityInfo boughtCommInfo = new HistoricalCommodityInfo();
        boughtCommInfo.setCommodityTypeAndKey(BOUGHT_COMMODITY_TYPE);
        boughtCommInfo.setHistoricalUsed(30);
        boughtCommInfo.setHistoricalPeak(40);
        boughtCommInfo.setSourceId(456);
        boughtCommInfo.setMatched(false);
        boughtCommInfo.setExisting(true);

        // Creating HistoricalServiceEntityInfo
        HistoricalServiceEntityInfo seInfo = new HistoricalServiceEntityInfo();
        seInfo.setSeOid(12345678);
        seInfo.setUsedHistoryWeight(0.5f);
        seInfo.setPeakHistoryWeight(0.99f);
        seInfo.getHistoricalCommoditySold().add(soldCommInfo);
        seInfo.getHistoricalCommodityBought().add(boughtCommInfo);

        // Creating HistoricalInfo
        HistoricalInfo info = new HistoricalInfo();
        info.put(0L, seInfo);

        // Creating HistoricalUtilizationDatabase
        HistoricalUtilizationDatabase db = new HistoricalUtilizationDatabase(dsl);

        // Saving the BLOB
        db.saveInfo(info);

        // Reading the BLOB
        HistoricalInfoRecord record = db.getInfo();
        if (record != null) {
            byte[] bytes = record.getInfo();
            if (bytes != null) {
                HistoricalInfoDTO histInfo = null;
                try {
                    histInfo = HistoricalInfoDTO.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    //logger.error(e.getMessage());
                }
                resultInfo = Conversions.convertFromDto(histInfo);
            }
        }

        // Assertions
        HistoricalServiceEntityInfo resultSeInfo = resultInfo.get(12345678);
        assertNotNull(resultSeInfo);
        assertEquals(12345678, resultSeInfo.getSeOid());
        assertEquals(0.5, resultSeInfo.getUsedHistoryWeight(), 0.00001);
        assertEquals(0.99, resultSeInfo.getPeakHistoryWeight(), 0.00001);

        HistoricalCommodityInfo resultSoldCommInfo = resultSeInfo.getHistoricalCommoditySold().get(0);
        assertNotNull(resultSoldCommInfo);
        assertEquals(SOLD_COMMODITY_TYPE, soldCommInfo.getCommodityTypeAndKey());
        assertEquals(10, soldCommInfo.getHistoricalUsed(), 0.00001);
        assertEquals(20, soldCommInfo.getHistoricalPeak(), 0.00001);
        assertEquals(123, soldCommInfo.getSourceId());
        assertTrue(soldCommInfo.getMatched());
        assertFalse(soldCommInfo.getExisting());

        HistoricalCommodityInfo resultBoughtCommInfo = resultSeInfo.getHistoricalCommodityBought().get(0);
        assertNotNull(resultBoughtCommInfo);
        assertEquals(BOUGHT_COMMODITY_TYPE, boughtCommInfo.getCommodityTypeAndKey());
        assertEquals(30, boughtCommInfo.getHistoricalUsed(), 0.00001);
        assertEquals(40, boughtCommInfo.getHistoricalPeak(), 0.00001);
        assertEquals(456, boughtCommInfo.getSourceId());
        assertFalse(boughtCommInfo.getMatched());
        assertTrue(boughtCommInfo.getExisting());
    }
}
