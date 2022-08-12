package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.LegacyTopology;

/**
 * A test case for the {@link M2Utils} class.
 */
@RunWith(JUnitParamsRunner.class)
public class M2UtilsTest {

    /*
     * verify that sellers and buyers are in the basket/market where they are expected to be
     * verify placement
     * verify that PMs don't buy commodities from storage
     * verify that applications don't buy from VMs
     */

    private static Logger loggerOff = LogManager.getLogger("Test.OFF");
    private static Logger loggerTrace = LogManager.getLogger("Test.TRACE");
    static {
        LoggerContext ctx = (LoggerContext)LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig1 = config.getLoggerConfig(loggerOff.getName());
        loggerConfig1.setLevel(Level.OFF);
        LoggerConfig loggerConfig2 = config.getLoggerConfig(loggerTrace.getName());
        loggerConfig2.setLevel(Level.TRACE);
        ctx.updateLoggers();
    }

    private static final String XML_TOP = resourceToString("xml_top.xml");
    private static final String XML_BOTTOM = resourceToString("xml_bottom.xml");
    private static final String DC_2 = resourceToString("dc-2.xml");
    private static final String VM_1277 = resourceToString("vm-1277.xml");
    private static final String VM_733 = resourceToString("vm-733.xml");
    private static final String VM_1544 = resourceToString("vm-1544.xml");
    private static final String PM_9 = resourceToString("host-9.xml");
    private static final String DS_902 = resourceToString("datastore-902.xml");
    private static final String APP_733 = resourceToString("GuestLoad-vm-733.xml");
    private static final String EDGE = resourceToString("edge-cases.xml");
    private static final String BICLIQUES = resourceToString("bicliques.xml");

    // Test that a full file loads with no exception.
    @Test
    @Ignore // need to find a real file that is small enough to include in SVN and test with
    public void testLoadFullFile() {
        try {
            M2Utils.loadFile("data/repos/small.repos.topology");
        } catch (IOException | ParseException | ParserConfigurationException e) {
            // Have to catch this because the method throws it. Other exceptions will implicitly fail the test.
            fail("File not found : " + e);
        }
    }

    @Test
    public void testLoadEmpty() throws ParseException, IOException, ParserConfigurationException {
        LegacyTopology topology = loadString(XML_TOP + XML_BOTTOM);
        assertEquals(0, topology.getEconomy().getTraders().size());
        assertEquals(0, topology.getEconomy().getMarkets().size());
    }

    /*
     * Test a file that includes some edge cases:
     * - No Consumes
     * - Multiple Consumes
     * - used > capacity
     * - peakUtil > 1
     *
     * Use loggerTrace to increase code coverage
     */

    @Test
    public void testEdgeCases() throws ParseException, IOException, ParserConfigurationException {
        LegacyTopology topology = loadString(XML_TOP + EDGE + XML_BOTTOM, loggerTrace);
        assertEquals(1, topology.getEconomy().getTraders().size());
        assertEquals(0, topology.getEconomy().getMarkets().size());
    }

    /**
     * Test that DSPMAccessCommodity and DatastoreCommodity are replaced with BC-DS and
     * BC-PM commodities.
     */
    @Test
    public void testBicliques() throws ParseException, IOException, ParserConfigurationException {
        /*
         * The file contains 8 commodity types: 1 CPU, 1 StorageAmount, DSPMAccessCommodity
         * with 3 different keys, and DatastoreCommodity with 3 different keys.
         * The algorithm replaces all the DSPMAccessCommodity and DatastoreCommodity with one
         * biclique that has two biclique commodities: BC-DS-0 and BC-PM-0. Assert that each
         * market (there are two) has exactly two commodity types (for a total of 4 commodity
         * types).
         */
        LegacyTopology topology = loadString(XML_TOP + BICLIQUES + XML_BOTTOM, loggerTrace);
        Market[] markets = topology.getEconomy().getMarkets().toArray(new Market[]{});
        assertEquals(2, markets.length);
        for (@NonNull @ReadOnly Market market : markets) {
            // 2 commodity types in each basket
            assertEquals(2, market.getBasket().size());
            // 3 sellers (the 3 PMs or 3 DSs)
            assertEquals(3, market.getActiveSellers().size());
            // one buyer (the VM)
            assertEquals(1, market.getBuyers().size());
        }
        // Verify that the baskets are mutually exclusive
        assertTrue(markets[0].getBasket().contains(new CommoditySpecification(
                  topology.getCommodityTypes().getId("Abstraction:StorageAmount"))));
        assertTrue(markets[0].getBasket().contains(new CommoditySpecification(
                  topology.getCommodityTypes().getId("BC-DS-0"))));
        assertTrue(markets[1].getBasket().contains(new CommoditySpecification(
                  topology.getCommodityTypes().getId("Abstraction:CPU"))));
        assertTrue(markets[1].getBasket().contains(new CommoditySpecification(
                  topology.getCommodityTypes().getId("BC-PM-0"))));
    }

    /**
     * Test that loading one entity results in one trader and no markets
     */
    @Parameters
    @TestCaseName("Test #{index}: Load {0}")
    @Test
    public void testLoadOne(String name, String entity) throws ParseException, IOException, ParserConfigurationException {
        LegacyTopology topology = loadString(XML_TOP + entity + XML_BOTTOM);
        assertEquals(1, topology.getEconomy().getTraders().size());
        assertEquals(0, topology.getEconomy().getMarkets().size());
    }

    @SuppressWarnings("unused")
    private static String[][] parametersForTestLoadOne() {
        return new String[][]{
            {"vm-1277", VM_1277},
            {"vm-733", VM_733},
            {"vm-1544", VM_1544},
            {"host-9", PM_9},
            {"datacenter-2", DC_2},
            {"datastore-902", DS_902},
            {"GuestLoad-vm-733", APP_733}
        };
    }

    @Parameters
    @TestCaseName("Test #{index}: Load {0}")
    @Test
    public void testLoadSellerAndBuyer(String name, String seller, String buyer)
            throws ParseException, IOException, ParserConfigurationException {
        // This test is run with loggerTrace to include logging in code coverage
        LegacyTopology topology1 = loadString(XML_TOP + seller + buyer + XML_BOTTOM, loggerTrace);
        assertEquals(2, topology1.getEconomy().getTraders().size());
        assertEquals(1, topology1.getEconomy().getMarkets().size());
        // Test that loading in the reverse order gives the same results
        LegacyTopology topology2 = loadString(XML_TOP + buyer + seller + XML_BOTTOM);
        assertEquals(2, topology2.getEconomy().getTraders().size());
        assertEquals(1, topology2.getEconomy().getMarkets().size());
    }

    @SuppressWarnings("unused")
    private static String[][] parametersForTestLoadSellerAndBuyer() {
        return new String[][]{
            {"host-9 + vm-1277", PM_9, VM_1277},
            {"host-9 + vm-733", PM_9, VM_733},
            {"datastore-902 + vm-733", DS_902, VM_733},
            {"datacenter-2 + host-9", DC_2, PM_9}
        };
    }

    @Parameters
    @TestCaseName("Test #{index}: Load {0}")
    @Test
    public void testLoadDisconnectedTraders(String name, String trader1, String trader2)
            throws ParseException, IOException, ParserConfigurationException {
        LegacyTopology topology = loadString(XML_TOP + trader1 + trader2 + XML_BOTTOM);
        assertEquals(2, topology.getEconomy().getTraders().size());
        assertEquals(0, topology.getEconomy().getMarkets().size());
    }

    @SuppressWarnings("unused")
    private static String[][] parametersForTestLoadDisconnectedTraders() {
        return new String[][]{
            {"host-9 + datastore-902", PM_9, DS_902},
            {"vm-1277 + vm-733", VM_1277, VM_733},
            {"datacenter-2 + datastore-902", DC_2, DS_902}
        };
    }

    @Test
    public void testCommodityValues() throws ParseException, IOException, ParserConfigurationException {
        LegacyTopology topology = loadString(XML_TOP + DS_902 + XML_BOTTOM);
        UnmodifiableEconomy economy = topology.getEconomy();
        Trader ds = economy.getTraders().get(0);
        // StorageAmount
        CommoditySold storageAmount = ds.getCommoditySold(new CommoditySpecification(
             topology.getCommodityTypes().getId("Abstraction:StorageAmount")));
        assertEquals(673442.4, storageAmount.getCapacity(), TestUtils.FLOATING_POINT_DELTA);
        assertEquals(96092.0, storageAmount.getQuantity(), TestUtils.FLOATING_POINT_DELTA);
        assertEquals(0.0, storageAmount.getPeakQuantity(), TestUtils.FLOATING_POINT_DELTA);
        assertEquals(0.14268778, storageAmount.getUtilization(), TestUtils.FLOATING_POINT_DELTA);

        CommoditySoldSettings storageAmountSettings = storageAmount.getSettings();
        assertEquals(0.9, storageAmountSettings.getUtilizationUpperBound(), TestUtils.FLOATING_POINT_DELTA);
        // StorageAccess
        CommoditySold storageAccess = ds.getCommoditiesSold().get(1);
        CommoditySoldSettings storageAccessSettings = storageAccess.getSettings();
        // When unset, utilThreshold should be 1.0
        assertEquals(1.0, storageAccessSettings.getUtilizationUpperBound(), TestUtils.FLOATING_POINT_DELTA);
    }

    @Test
    public void testTopology() throws ParseException, IOException, ParserConfigurationException {
        LegacyTopology topology = loadString(
                XML_TOP + VM_1277 + VM_733 + PM_9 + DS_902 + APP_733 + VM_1544 + DC_2 + XML_BOTTOM
            );
        /*
         * Expecting 7 traders in this topology: 3 VMs (trader type 0), one PM (1), one storage (2),
         * one application (3) and one datacenter (4)
         * Expecting 4 markets:
         * datacenter-2 sells Power, Cooloing and Space to host-9
         * host-9 selling CPU, Mem and Q2CPU to vm-1277
         * host-9 selling CPU, Mem, Q4CPU and DatastoreCommodity to vm-733
         * datastore-902 selling StorageAmount and DSPMAccessCommodity to vm-733
         *
         * vm-1544 buys CPU, Q2VCPU and Mem - but from a host that is not in the topology.
         * host not supposed to buy from storage although the commodities are in the files
         * application app-733 not supposed to buy from vm-733 although the commodities are in the files
         *
         */
        UnmodifiableEconomy economy = topology.getEconomy();
        List<@NonNull @ReadOnly Trader> traders = economy.getTraders();
        // 6 traders - one in each file
        assertEquals(traders.size(), 7);
        // 3 markets
        assertEquals(economy.getMarkets().size(), 4);
        Trader vm_1277 = traders.get(0);
        Trader vm_733 = traders.get(1);
        Trader pm_9 = traders.get(2);
        Trader ds_902= traders.get(3);
        Trader app_733 = traders.get(4);
        Trader vm_1544 = traders.get(5);
        Trader dc_2 = traders.get(6);

        // vm-1277 buying only from from the host
        assertEquals(1, economy.getSuppliers(vm_1277).size());
        assertTrue(economy.getSuppliers(vm_1277).contains(pm_9));
        // vm-733 buys one basket from the host and one from the datastore
        assertEquals(2, economy.getSuppliers(vm_733).size());
        assertTrue(economy.getSuppliers(vm_733).contains(pm_9));
        assertTrue(economy.getSuppliers(vm_733).contains(ds_902));
        // same as last two checks - but tests a different method
        assertTrue(pm_9.getUniqueCustomers().contains(vm_733));
        assertTrue(ds_902.getUniqueCustomers().contains(vm_733));
        // vm-1544 not selling and not buying in any market
        assertTrue(economy.getSuppliers(vm_1544).isEmpty());
        assertTrue(vm_1544.getUniqueCustomers().isEmpty());
        // host-9 buying from datacenter-2
        assertTrue(economy.getSuppliers(pm_9).contains(dc_2));

        // PM does not buy from Storage
        assertFalse(ds_902.getUniqueCustomers().contains(pm_9));
        // VM not buying from App
        assertFalse(vm_733.getUniqueCustomers().contains(app_733));

        // TODO: add tests about specific commodities
    }

    private LegacyTopology loadString(String xml)
            throws ParseException, IOException, ParserConfigurationException {
        return loadString(xml, loggerOff);
    }

    private LegacyTopology loadString(String xml, Logger logger)
            throws ParseException, IOException, ParserConfigurationException {
        return M2Utils.loadStream(new ByteArrayInputStream(xml.getBytes()), logger);
    }

    private static String resourceToString(String fileName) {
        try {
            return Resources.toString(M2UtilsTest.class.getClassLoader()
                            .getResource("data/repos/" + fileName), Charsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalArgumentException("Test resource not found " + fileName, e);
        }
    }
}
