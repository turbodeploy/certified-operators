package com.vmturbo.market.runner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.runner.Analysis.AnalysisState;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.market.PriceIndexNotificationSender;

/**
 * Unit tests for the {@link MarketRunner}.
 */
public class MarketRunnerTest {

    MarketRunner runner;
    ExecutorService threadPool;
    MarketNotificationSender serverApi = Mockito.mock(MarketNotificationSender.class);
    PriceIndexNotificationSender priceIndexApi = Mockito.mock(PriceIndexNotificationSender.class);

    long topologyContextId = 1000;
    long topologyId = 2000;
    long creationTime = 3000;

    @Before
    public void before() {
        IdentityGenerator.initPrefix(0);
        threadPool = Executors.newFixedThreadPool(2);
        runner = new MarketRunner(threadPool, serverApi, priceIndexApi);
        topologyContextId += 100;
    }

    @After
    public void after() {
        threadPool.shutdownNow();
    }

    /**
     * Test that the constructor of {@link Analysis} initializes the fields as expected.
     * @throws InterruptedException because I use Thread.sleep(.)
     */
    @Test
    public void testGetRuns() throws InterruptedException {
        Analysis analysis =
                runner.scheduleAnalysis(topologyContextId, topologyId,
                    creationTime, dtos(true), TopologyType.PLAN, true);
        assertTrue(runner.getRuns().contains(analysis));
        while (!analysis.isDone()) {
            Thread.sleep(100);
        }
        assertSame("Plan completed with an error : " + analysis.getErrorMsg(),
            AnalysisState.SUCCEEDED, analysis.getState());
        Mockito.verify(serverApi, Mockito.times(1)).notifyActionsRecommended(analysis.getActionPlan().get());
        // since the IDgenerator gives us a different projectedTopoID every time, we create a
        // MockitoMatcher using anyLong to represent this parameter
        Mockito.verify(serverApi, Mockito.times(1))
                .notifyProjectedTopology(Mockito.eq(topologyId), Mockito.anyLong(), Mockito.eq(topologyContextId),
                    Mockito.any(TopologyType.class), Mockito.eq(creationTime),
                        Mockito.eq(analysis.getProjectedTopology().get()));
        PriceIndexMessage pim = PriceIndexMessage.newBuilder(analysis.getPriceIndexMessage().get())
                        .setTopologyContextId(analysis.getContextId())
                        .build();
        Mockito.verify(priceIndexApi, Mockito.times(1)).sendPriceIndex(topologyId, creationTime, pim);
    }

    /**
     * Test that when trying to run the same topology context id twice, the first one gets queued,
     * and the second one returns the Analysis object of the first one.
     */
    @Test
    public void testContextIDs() {
        Set<TopologyEntityDTO> dtos = dtos(true);
        Analysis analysis1 = runner.scheduleAnalysis(topologyContextId, topologyId,
            creationTime, dtos, TopologyType.PLAN, true);
        Analysis analysis2 = runner.scheduleAnalysis(topologyContextId, topologyId,
            creationTime, dtos, TopologyType.PLAN, true);
        Analysis analysis3 = runner.scheduleAnalysis(topologyContextId + 1, topologyId,
            creationTime, dtos, TopologyType.PLAN, true);
        assertSame(analysis1, analysis2);
        assertNotSame(analysis1, analysis3);
    }

    /**
     * Test that a bad plan results in a FAILED state, and that a completed test is removed from the
     * list of runs.
     * @throws InterruptedException because I use Thread.sleep(.)
     */
    @Test
    public void testBadPlan() throws InterruptedException {
        Set<TopologyEntityDTO> badDtos = dtos(false);
        Analysis analysis = runner.scheduleAnalysis(topologyContextId, topologyId,
            creationTime, badDtos, TopologyType.PLAN, true);
        while (!analysis.isDone()) {
            Thread.sleep(100);
        }
        assertSame(AnalysisState.FAILED, analysis.getState());
        assertNotNull(analysis.getErrorMsg());
        assertFalse(runner.getRuns().contains(analysis));
    }

    private long entityCount = 0;
    private CommodityType commType = CommodityType.newBuilder().setType(1).build();

    /**
     * Construct a set of a entity DTOs representing a few buyers and one seller.
     * @param good when false, the buyers buy a negative amount,
     *     and the resulted plan ends with an error.
     * @return the set of entity DTOs
     */
    private Set<TopologyEntityDTO> dtos(boolean good) {
        Set<TopologyEntityDTO> dtos = Sets.newHashSet();
        TopologyEntityDTO seller = seller();
        dtos.add(seller);
        long sellerOid = seller.getOid();
        long used = good ? 1 : -1;
        for (int i = 0; i < 3; i++) {
            dtos.add(buyer(sellerOid, used));
        }
        return dtos;
    }

    /**
     * A TopologyEntityDTO that represents a seller.
     * @return a TopologyEntityDTO that represents a seller
     */
    private TopologyEntityDTO seller() {
        TopologyEntityDTO seller = TopologyEntityDTO.newBuilder()
                .setEntityType(2000) // This must not be a VDC, which is ignored in M2
                .setOid(entityCount++)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(commType)
                        .setCapacity(1)
                        .build())
                .build();
        return seller;
    }

    /**
     * A TopologyEntityDTO that represents a buyer.
     * @param sellerOid the oid of the seller that this buyer is buying from
     * @param used the amount bought
     * @return a TopologyEntityDTO that represents a buyer
     */
    private TopologyEntityDTO buyer(long sellerOid, long used) {
        TopologyEntityDTO buyer = TopologyEntityDTO.newBuilder()
            .setEntityType(1000) // This must not be a VDC, which is ignored in M2
            .setOid(entityCount++)
            .putCommodityBoughtMap(sellerOid, TopologyEntityDTO.CommodityBoughtList.newBuilder()
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setCommodityType(commType)
                            .setUsed(used)
                            .build()) // commodity bought
                    .build()) // commodity bought list
            .build(); // buyer
        return buyer;
    }
}
