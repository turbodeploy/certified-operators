package com.vmturbo.topology.processor.historical;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.topology.HistoricalInfo.HistoricalInfoDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.topology.processor.controllable.EntityActionDaoImpTest.TestTopologyProcessorDbEndpointConfig;
import com.vmturbo.topology.processor.db.TopologyProcessor;
import com.vmturbo.topology.processor.db.tables.HistoricalUtilization;
import com.vmturbo.topology.processor.db.tables.records.HistoricalUtilizationRecord;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestTopologyProcessorDbEndpointConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"sqlDialect=MARIADB"})
public class HistoricalUtilizationDatabaseTest {

    @Autowired(required = false)
    private TestTopologyProcessorDbEndpointConfig dbEndpointConfig;

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

    /**
     * Test rule to use {@link DbEndpoint}s in test.
     */
    @Rule
    public DbEndpointTestRule dbEndpointTestRule = new DbEndpointTestRule("tp");

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule().testAllCombos(
            FeatureFlags.POSTGRES_PRIMARY_DB);

    private static final CommodityType SOLD_COMMODITY_TYPE = CommodityType.newBuilder()
        .setType(1234)
        .setKey("2333")
        .build();

    private static final CommodityType BOUGHT_COMMODITY_TYPE = CommodityType.newBuilder()
        .setType(5678)
        .setKey("666")
        .build();

    private DSLContext dsl;

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

    /**
     * Set up before each test.
     *
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
     */
    @Before
    public void before() throws SQLException, UnsupportedDialectException, InterruptedException {
        if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
            dbEndpointTestRule.addEndpoints(dbEndpointConfig.tpEndpoint());
            dsl = dbEndpointConfig.tpEndpoint().dslContext();
        } else {
            dsl = dbConfig.getDslContext();
        }
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
