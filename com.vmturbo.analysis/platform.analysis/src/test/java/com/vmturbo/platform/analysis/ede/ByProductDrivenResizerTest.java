package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.commons.analysis.RawMaterialsMap;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.ByProducts;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.RawMaterials;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.updatingfunction.ProjectionFunctionFactory;
import com.vmturbo.platform.analysis.updatingfunction.UpdatingFunctionFactory;

/**
 * A collection of tests to evaluate the resizing decisions for a container that has a byProduct commodity.
 */
@RunWith(JUnitParamsRunner.class)
public class ByProductDrivenResizerTest {

    TestCommon testEconomy;
    Trader app;
    Trader cont;
    Trader pod;
    Ledger ledger;
    CommoditySpecification drivingComm = new CommoditySpecification(0).setDebugInfoNeverUseInCode("DRIVER");
    CommoditySpecification byProductComm = new CommoditySpecification(1).setDebugInfoNeverUseInCode("BY-PRODUCT");
    CommoditySpecification rawMaterialComm = new CommoditySpecification(2).setDebugInfoNeverUseInCode("RAW_MATERIAL");

    private static final double RIGHT_SIZE_LOWER = 0.3;
    private static final double RIGHT_SIZE_UPPER = 0.7;

    private static final Logger logger = LogManager.getLogger(UpdatingFunctionFactory.class);

    /**
     * Create a Economy during setup.
     */
    @Before
    public void setUp() {
        testEconomy = new TestCommon();
    }

    /**
     * Parameterised test evaluating resizing decisions for a container selling a byProduct commodity for various
     * trader usages.
     * @param drivingCommOverhead existing consumption on driving commodity.
     * @param drivingCommConsumerUsage consumption on driving commodity due to a buyer.
     * @param byProductOverhead existing consumption on byProduct commodity.
     * @param rawMaterialOverhead existing consumption on rawMaterial.
     * @param rawMaterialConsumerUsage consumption on rawMaterial due to the resizing seller.
     * @param numActions is the number of expected actions.
     * @param newCapacity is the new capacity of the resizing commodity.
     * */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: ByProductDrivenResizeDecisionsTest({0}, {1}, {2}, {3}, {4}, {5}, {6})")
    public final void testByProductDrivenResizeDecisions(double drivingCommOverhead,
                                                         double drivingCommConsumerUsage,
                                                         double byProductOverhead,
                                                         double rawMaterialOverhead,
                                                         double rawMaterialConsumerUsage,
                                                         int numActions,
                                                         double newCapacity) {
        Economy economy = new Economy();

        // Create pod
        pod = TestUtils.createTrader(economy, TestUtils.POD_TYPE,
                Arrays.asList(0L), Arrays.asList(rawMaterialComm),
                new double[]{200}, false, false);
        pod.setDebugInfoNeverUseInCode("POD1");
        pod.getCommoditiesSold().stream().forEach(cs -> cs.getSettings().setResizable(false));
        pod.getCommoditySold(rawMaterialComm).setQuantity(rawMaterialOverhead);

        // Create cont
        cont = TestUtils.createTrader(economy, TestUtils.CONTAINER_TYPE,
                Arrays.asList(0L), Arrays.asList(drivingComm, byProductComm),
                new double[]{100, 100}, false, false);
        cont.getCommoditySold(drivingComm).setQuantity(drivingCommOverhead).getSettings().setCapacityIncrement(5);
        cont.getCommoditySold(byProductComm).setQuantity(byProductOverhead);
        cont.getSettings().setMinDesiredUtil(0.65).setMaxDesiredUtil(0.75);
        cont.setDebugInfoNeverUseInCode("CONTAINER1");

        // place container on pod
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(rawMaterialComm), cont,
                new double[]{rawMaterialConsumerUsage}, pod).setMovable(false);

        // Create app
        app = TestUtils.createTrader(economy, TestUtils.APP_TYPE,
                Arrays.asList(0L), Arrays.asList(), new double[]{}, false, false);
        app.setDebugInfoNeverUseInCode("APP1");

        // place app on container
        TestUtils.createAndPlaceShoppingList(economy,
                Arrays.asList(drivingComm), app,
                new double[]{drivingCommConsumerUsage}, cont);

        economy.getSettings().setRightSizeLower(RIGHT_SIZE_LOWER);
        economy.getSettings().setRightSizeUpper(RIGHT_SIZE_UPPER);
        economy.getModifiableRawCommodityMap().put(drivingComm.getBaseType(),
                new RawMaterials(new RawMaterialsMap.RawMaterialInfo(
                    ImmutableList.of(new RawMaterialsMap.RawMaterial(rawMaterialComm.getBaseType(), false, false)))));

        economy.getModifiableByProductMap().put(drivingComm.getBaseType(), new ByProducts(ImmutableMap.of(byProductComm.getBaseType(),
                        ProjectionFunctionFactory.MM1)));

        TestUtils.setupRawCommodityMap(economy);
        TestUtils.setupCommodityResizeDependencyMap(economy);
        ledger = new Ledger(economy);

        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        List<Action> actions = Resizer.resizeDecisions(economy, ledger);
        assertEquals(numActions, actions.size());
        if (numActions == 1) {
            Resize resize = (Resize)actions.get(0);
            // assert resize up
            if (newCapacity > resize.getNewCapacity()) {
                assertTrue(newCapacity == resize.getNewCapacity());
            }
        }
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestByProductDrivenResizeDecisions() {
        return new Object[][] {
                // drivingCommOverhead, drivingCommConsumerUsage, byProductOverhead, rawMaterialOverhead, rawMaterialConsumerUsage
                {0, 30, 80, 0, 30, 1, 115},
                {20, 30, 80, 0, 30, 1, 110},
                {70, 10, 80, 0, 30, 1, 120},
                {50, 30, 30, 0, 30, 1, 115},
                {0, 80, 30, 0, 30, 1, 115},
                {0, 80, 0, 0, 30, 1, 115},

                // change in behavior btw rawMaterial util of 10%-40%.
                // no resize down with byProd.
                {0, 20, 30, 0, 80, 0, 0},
                // resize down without byProd.
                {0, 20, 0, 0, 80, 1, 0},
                // resize down with and without byProd converges around rawMat util of 42.5%.
                {0, 20, 30, 0, 85, 1, 0},
                {0, 20, 0, 0, 85, 1, 0},

                // change in behavior btw rawMaterial util of 27.5%-47.5%.
                {0, 30, 30, 0, 55, 0, 0},
                {0, 30, 0, 0, 55, 1, 0},
                // no resize down with byProd.
                {0, 30, 30, 0, 95, 0, 0},
                // resize down with byProd.
                {0, 30, 0, 0, 95, 1, 0},
                // resize down with and without byProd.
                {0, 30, 30, 0, 100, 1, 0},
                {0, 30, 0, 0, 100, 1, 0},

                // Change in behavior btw rawMaterial util of 60%-65%.
                // no resize down with byProd.
                {0, 50, 30, 0, 120, 0, 0},
                // resize down without byProd.
                {0, 50, 0, 0, 120, 1, 0},
                // resize down with and without byProd.
                {0, 50, 30, 0, 130, 1, 0},
                {0, 50, 0, 0, 130, 1, 0}
        };
    }
}

