package com.vmturbo.topology.processor.historical;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.HistoricalInfo.HistoricalInfoDTO;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.sql.utils.MultiDbTestBase;
import com.vmturbo.topology.processor.TestTopologyProcessorDbEndpointConfig;
import com.vmturbo.topology.processor.db.TopologyProcessor;
import com.vmturbo.topology.processor.db.tables.HistoricalUtilization;
import com.vmturbo.topology.processor.db.tables.records.HistoricalUtilizationRecord;

@RunWith(Parameterized.class)
public class HistoricalUtilizationDatabaseTest extends MultiDbTestBase {

    /**
     * Provide test parameter values.
     *
     * @return parameter values
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public HistoricalUtilizationDatabaseTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(TopologyProcessor.TOPOLOGY_PROCESSOR, configurableDbDialect, dialect, "topology-processor",
                TestTopologyProcessorDbEndpointConfig::tpEndpoint);
        this.dsl = super.getDslContext();
    }

    private static final CommodityTypeView SOLD_COMMODITY_TYPE = new CommodityTypeImpl()
            .setType(1234)
            .setKey("2333");

    private static final CommodityTypeView BOUGHT_COMMODITY_TYPE = new CommodityTypeImpl()
        .setType(5678)
        .setKey("666");

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
