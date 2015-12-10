package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.utilities.M2Utils.TopologyMapping;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

@RunWith(JUnitParamsRunner.class)
public class TestLoadFile {

    /*
     * verify that sellers and buyers are in the basket/market where they are expected to be
     * verify placement
     * verify that PMs don't buy commodities from storage
     * verify that applications don't buy from VMs
     */

    private static Logger loggerOff = Logger.getLogger("Test");
    static {
        loggerOff.setLevel(Level.OFF);
    }

    private static final String REPOS_PATH = "src/test/resources/data/repos/";

    private static final String XML_TOP = fileToString(REPOS_PATH + "xml_top.xml");
    private static final String XML_BOTTOM = fileToString(REPOS_PATH + "xml_bottom.xml");
    private static final String VM_1277 = fileToString(REPOS_PATH + "vm-1277.xml");
    private static final String VM_733 = fileToString(REPOS_PATH + "vm-733.xml");
    private static final String VM_1544 = fileToString(REPOS_PATH + "vm-1544.xml");
    private static final String PM_9 = fileToString(REPOS_PATH + "host-9.xml");
    private static final String DS_902 = fileToString(REPOS_PATH + "datastore-902.xml");
    private static final String APP_733 = fileToString(REPOS_PATH + "GuestLoad-vm-733.xml");

    // Test that a full file loads with no exception.
    @Test
    @Ignore // need to find a real file that is small enough to include in SVN and test with
    public void testLoadFullFile() {
        try {
            M2Utils.loadFile(REPOS_PATH + "small.repos.topology", loggerOff);
        } catch (FileNotFoundException e) {
            // Have to catch this because the method throws it. Other exceptions will implicitly fail the test.
            fail("File not found : " + e);
        }
    }

    @Test
    public void testLoadEmpty() {
        TopologyMapping topoMap = loadString(XML_TOP + XML_BOTTOM);
        assertEquals(0, topoMap.getTopology().getEconomy().getTraders().size());
        assertEquals(0, topoMap.getTopology().getEconomy().getMarkets().size());
    }

    /**
     * Tast that loading one entity results in one trader and no markets
     */
    @Parameters
    @TestCaseName("Test #{index}: Load {0}")
    @Test
    public void testLoadOne(String name, String entity) {
        TopologyMapping topoMap = loadString(XML_TOP + entity + XML_BOTTOM);
        assertEquals(1, topoMap.getTopology().getEconomy().getTraders().size());
        assertEquals(0, topoMap.getTopology().getEconomy().getMarkets().size());
    }

    @SuppressWarnings("unused")
    private static String[][] parametersForTestLoadOne() {
        return new String[][]{
            {"vm-1277", VM_1277},
            {"vm-733", VM_733},
            {"host-9", PM_9},
            {"datastore-902", DS_902},
            {"GuestLoad-vm-733", APP_733}
        };
    }

    @Parameters
    @TestCaseName("Test #{index}: Load {0}")
    @Test
    public void testLoadSellerAndBuyer(String name, String seller, String buyer) {
        TopologyMapping topoMap = loadString(XML_TOP + seller + buyer + XML_BOTTOM);
        assertEquals(2, topoMap.getTopology().getEconomy().getTraders().size());
        assertEquals(1, topoMap.getTopology().getEconomy().getMarkets().size());
    }

    @SuppressWarnings("unused")
    private static String[][] parametersForTestLoadSellerAndBuyer() {
        return new String[][]{
            {"host-9 + vm-1277", PM_9, VM_1277},
            {"host-9 + vm-733", PM_9, VM_733},
            {"datastore-902 + vm-733", DS_902, VM_733}
        };
    }

    @Parameters
    @TestCaseName("Test #{index}: Load {0}")
    @Test
    public void testLoadDisconnectedTraders(String name, String trader1, String trader2) {
        TopologyMapping topoMap = loadString(XML_TOP + trader1 + trader2 + XML_BOTTOM);
        assertEquals(2, topoMap.getTopology().getEconomy().getTraders().size());
        assertEquals(0, topoMap.getTopology().getEconomy().getMarkets().size());
    }

    @SuppressWarnings("unused")
    private static String[][] parametersForTestLoadDisconnectedTraders() {
        return new String[][]{
            {"host-9 + datastore-902", PM_9, DS_902},
            {"vm-1277 + vm-733", VM_1277, VM_733}
        };
    }

    @Test
    public void testCommodityValues() {
        TopologyMapping topoMap = loadString(XML_TOP + DS_902 + XML_BOTTOM);
        Economy economy = topoMap.getTopology().getEconomy();
        Trader ds = economy.getTraders().get(0);
        // StorageAmount
        CommoditySold storageAmount = ds.getCommoditiesSold().get(0);
        assertEquals(673442.4, storageAmount.getCapacity(), 1e-9);
        assertEquals(96092.0, storageAmount.getQuantity(), 1e-9);
        assertEquals(0.0, storageAmount.getPeakQuantity(), 1e-9);
        assertEquals(0.14268778, storageAmount.getUtilization(), 1e-5);

        CommoditySoldSettings storageAmountSettings = storageAmount.getSettings();
        assertEquals(0.9, storageAmountSettings.getUtilizationUpperBound(), 1e-9);
        // StorageAccess
        CommoditySold storageAccess = ds.getCommoditiesSold().get(1);
        CommoditySoldSettings storageAccessSettings = storageAccess.getSettings();
        // When unset, utilThreshold should be 1.0
        assertEquals(1.0, storageAccessSettings.getUtilizationUpperBound(), 1e-9);
    }

    @Test
    public void testTopology() {
        TopologyMapping topoMap = loadString(XML_TOP + VM_1277 + VM_733 + PM_9 + DS_902 + APP_733 + VM_1544 + XML_BOTTOM);
        /*
         * Expecting 6 traders in this topology: 3 VMs (trader type 0), one PM (1), one storage (2) and one application (3).
         * Expecting 3 markets:
         * host-9 selling CPU, Mem and Q2CPU to vm-1277
         * host-9 selling CPU, Mem, Q4CPU and DatastoreCommodity to vm-733
         * datastore-902 selling StorageAmount and DSPMAccessCommodity to vm-733
         *
         * vm-1544 buys CPU, Q2VCPU and Mem - but from a host that is not in the topology.
         * host not supposed to buy from storage although the commodities are in the files
         * application app-733 not supposed to buy from vm-733 although the commodities are in the files
         *
         */
        inspect(topoMap);
        Economy economy = topoMap.getTopology().getEconomy();
        List<@NonNull @ReadOnly Trader> traders = economy.getTraders();
        // 6 traders - one in each file
        assertEquals(traders.size(), 6);
        // 3 markets
        assertEquals(economy.getMarkets().size(), 3);
        Trader vm_1277 = traders.get(0);
        Trader vm_733 = traders.get(1);
        Trader pm_9 = traders.get(2);
        Trader ds_902= traders.get(3);
        Trader app_733 = traders.get(4);
        Trader vm_1544 = traders.get(5);

        // vm-1277 buying only from from the host
        assertEquals(1, economy.getSuppliers(vm_1277).size());
        assertTrue(economy.getSuppliers(vm_1277).contains(pm_9));
        // vm-733 buys one basket from the host and one from the datastore
        assertEquals(2, economy.getSuppliers(vm_733).size());
        assertTrue(economy.getSuppliers(vm_733).contains(pm_9));
        assertTrue(economy.getSuppliers(vm_733).contains(ds_902));
        // same as last two checks - but tests a different method
        assertTrue(economy.getCustomers(pm_9).contains(vm_733));
        assertTrue(economy.getCustomers(ds_902).contains(vm_733));
        // vm-1544 not selling and not buying in any market
        assertTrue(economy.getSuppliers(vm_1544).isEmpty());
        assertTrue(economy.getCustomers(vm_1544).isEmpty());

        // PM does not buy from Storage
        assertFalse(economy.getCustomers(ds_902).contains(pm_9));
        // VM not buying from App
        assertFalse(economy.getCustomers(vm_733).contains(app_733));

        // TODO: add tests about specific commodities
    }

    private TopologyMapping loadString(String xml) {
        return M2Utils.loadStream(new ByteArrayInputStream(xml.getBytes()), loggerOff);
    }

    private void inspect(TopologyMapping tmap) {
        Economy economy = tmap.getTopology().getEconomy();
        @NonNull @ReadOnly Collection<@NonNull @ReadOnly Market> markets = economy.getMarkets();
        System.out.println("Traders:");
        for (Trader trader : economy.getTraders()) {
            int traderIndex = economy.getIndex(trader);
            System.out.println("#" + traderIndex + ". (type " + trader.getType() + ") "+ tmap.getTraderName(traderIndex));
        }
        System.out.println(markets.size() + " markets");
        for (@NonNull @ReadOnly Market market : markets) {
            System.out.println(market.getBasket());
            System.out.println("    Sellers:");
            @NonNull @ReadOnly List<@NonNull Trader> sellers = market.getSellers();
            for (Trader seller : sellers) {
                System.out.println("        " + traderName(seller, tmap));
            }
            System.out.println("    Participations:");
            @NonNull @ReadOnly List<@NonNull BuyerParticipation> participations = market.getBuyers();
            Set<Trader> buyers = new HashSet<>();
            for (BuyerParticipation participation : participations) {
                Trader buyer = participation.getBuyer();
                buyers.add(buyer);
                System.out.println("        " + traderName(buyer, tmap));
            }
        }
    }

    /*
     * Currently a buyer can have only one participation per market
     * TODO: create multiple participations when valid (and test it)
     */

    // TODO: Is there a way to get from the Trader to the Economy?
    private String traderName(Trader trader, TopologyMapping topoMap) {
        return topoMap.getTraderName(topoMap.getTopology().getEconomy().getIndex(trader));
    }

    private static String fileToString(String fileName) {
        try {
            return new String(
                    java.nio.file.Files.readAllBytes(
                        java.nio.file.Paths.get(fileName)
                    )
                );
        } catch (IOException ioe) {
            ioe.printStackTrace();
            fail("Exception trying to load file : " + ioe);
            return null;
        }
    }
}