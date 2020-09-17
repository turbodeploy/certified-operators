package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;

import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigratedWorkloadPlacement;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.component.history.HistoricalStatsService;
import com.vmturbo.platform.common.dto.CommonDTOREST;

/**
 * Tests the ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy class.
 * Note that it loads a test Spring configuration from the ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTestConfig
 * class, which defines a mock HistoricalStatsService and a MigratedWorkloadCloudCommitmentAlgorithmStrategy with the
 * mock HistoricalStatsService wired into it. It is gated by the ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTest
 * profile.
 */
@ActiveProfiles("ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTest")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTestConfig.class})
public class ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTest {

    private static final Logger logger = LogManager.getLogger(ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTest.class);

    /**
     * A mock HistoricalStatsService.
     */
    @Autowired
    private HistoricalStatsService historicalStatsService;

    /**
     * The MigratedWorkloadCloudCommitmentAlgorithmStrategy under test.
     */
    @Autowired
    private MigratedWorkloadCloudCommitmentAlgorithmStrategy migratedWorkloadCloudCommitmentAlgorithmStrategy;

    /**
     * Tests a VM with a set of max CPU values that fall below the threshold for identifying the VM as active.
     * No RI actions should be generated.
     */
    @Test
    @Ignore
    public void testOneVMNoRIActionsGenerated() {
        // Create the mock data we want the historical stats service to return
        Stats.EntityStats entityStats = createEnityStatsThatWillNotBuyRI(1);

        // Return a single mock entity stats that has 21 days of values, all below 0.20
        Mockito.when(historicalStatsService.getHistoricalStats(anyList(), anyList(), anyInt()))
                .thenReturn(Arrays.asList(entityStats));

        // Execute our strategy
        List<ActionDTO.Action> actions = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyze(Arrays.asList(createMockMigratedWorkloadPlacement(1)), null, null, null, null);
//        List<ReservedInstanceAnalysisRecommendation> actions = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyze(Arrays.asList(createMockMigratedWorkloadPlacement(1)), null);

        // Assert our results
        Assert.assertNotNull(actions);
        Assert.assertEquals("We should not generate any Buy RI actions", 0, actions.size());
    }

    /**
     * Tests a VM with a set of max CPU values that are all above 20%, so an RI should be generated.
     */
    @Test
    @Ignore
    public void testOneVMRIActionsGenerated() {
        // Create the mock data we want the historical stats service to return
        Stats.EntityStats entityStats = createEnityStatsThatWillBuyRI(1);

        // Return a single mock entity stats that has 21 days of values, all below 0.20
        Mockito.when(historicalStatsService.getHistoricalStats(anyList(), anyList(), anyInt()))
                .thenReturn(Arrays.asList(entityStats));

        // Execute our strategy
//        List<ReservedInstanceAnalysisRecommendation> actions = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyze(Arrays.asList(createMockMigratedWorkloadPlacement(1)), null);
        List<ActionDTO.Action> actions = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyze(Arrays.asList(createMockMigratedWorkloadPlacement(1)), null, null, null, null);
        Assert.assertNotNull(actions);

        // Assert our results
        Assert.assertEquals("We should generate one Buy RI action", 1, actions.size());
    }

//    @Test
//    public void testOneVMRIActionsGenerated() {
//        // Create the mock data we want the historical stats service to return
//        Stats.EntityStats entityStats = createEnityStatsThatWillBuyRI(1);
//
//        // Execute our strategy
////        List<ReservedInstanceAnalysisRecommendation> actions = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyze(Arrays.asList(createMockMigratedWorkloadPlacement(1)), null);
//        List<Long> oids = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyzeStatistics(Arrays.asList(entityStats));
//        Assert.assertNotNull(oids);
//
//        // Assert our results
//        Assert.assertEquals("We should generate one Buy RI action", 1, oids.size());
//    }

    /**
     * Tests two VMs that should both generate Buy RI actions.
     */
    @Test
    @Ignore
    public void testTwoVMsRIActionsGenerated() {
        // Return a two entity stats that have 21 days of values that are all above 20%
        Mockito.when(historicalStatsService.getHistoricalStats(anyList(), anyList(), anyInt()))
                .thenReturn(Arrays.asList(createEnityStatsThatWillBuyRI(1), createEnityStatsThatWillBuyRI(2)));

        // Execute our strategy
        List<ActionDTO.Action> actions = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyze(
                Arrays.asList(createMockMigratedWorkloadPlacement(1), createMockMigratedWorkloadPlacement(2)), null, null, null, null);
        Assert.assertNotNull(actions);

        // Assert our results
        Assert.assertEquals("We should generate two Buy RI actions", 2, actions.size());
    }

    /**
     * Tests two VMs, one that should generate a Buy RI action and one that should not.
     */
    @Test
    @Ignore
    public void testTwoVMsOneRIActionGenerated() {
        // Return a two entity stats that have 21 days of values that are all above 20%
        Mockito.when(historicalStatsService.getHistoricalStats(anyList(), anyList(), anyInt()))
                .thenReturn(Arrays.asList(createEnityStatsThatWillBuyRI(1), createEnityStatsThatWillNotBuyRI(2)));

        // Execute our strategy
        List<ActionDTO.Action> actions = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyze(Arrays.asList(createMockMigratedWorkloadPlacement(1)), null, null, null, null);
        Assert.assertNotNull(actions);

        // Assert our results
        Assert.assertEquals("We should generate one Buy RI action", 1, actions.size());
    }

    /**
     * Helper method that creates entity stats that will not generate a Buy RI action.
     *
     * @param id The ID of the VM.
     * @return Entity stats that will not generate a Buy RI action.
     */
    private Stats.EntityStats createEnityStatsThatWillNotBuyRI(long id) {
        return createMockEntityStats(id, 10f, 10f, 10f, 10f, 10f, 10f, 10f, 10f, 10f, 10f, 10f, 10f, 10f,
                10f, 10f, 10f, 10f, 10f, 10f, 10f, 10f);
    }

    /**
     * Helper method that creates entity stats that will generate a Buy RI action.
     *
     * @param id The ID of the VM.
     * @return Entity stats that will generate a Buy RI action.
     */
    private Stats.EntityStats createEnityStatsThatWillBuyRI(long id) {
        return createMockEntityStats(id, 30f, 30f, 30f, 30f, 30f, 30f, 30f, 30f, 30f, 30f, 30f, 30f, 30f,
                30f, 30f, 30f, 30f, 30f, 30f, 30f, 30f);
    }

    /**
     * Creates a mock MigratedWorkloadPlacement object whose virtual machine has the specified OID.
     *
     * @param vmOid The OID of the MigratedWorkloadPlacement's virtual machine
     * @return A mock MigratedWorkloadPlacement
     */
    private MigratedWorkloadPlacement createMockMigratedWorkloadPlacement(long vmOid) {
        return MigratedWorkloadPlacement.newBuilder()
                .setVirtualMachine(TopologyEntityDTO.newBuilder().setOid(vmOid).setEntityType(CommonDTOREST.EntityDTO.EntityType.VIRTUAL_MACHINE.getValue()).build())
                .build();
    }

    /**
     * Creates a mock EntityStats with the specified OID and the specified max CPU values.
     *
     * @param oid     The OID of the EntityStats
     * @param maxCpus The list of max CPU values that will ultimately be returned by contained StatValue mocks
     * @return A mock EntityStats
     */
    private Stats.EntityStats createMockEntityStats(long oid, float... maxCpus) {
        Stats.EntityStats.Builder builder = Stats.EntityStats.newBuilder();
        builder.setOid(oid);
        for (float maxCpu : maxCpus) {
            builder.addStatSnapshots(createMockStatSnapshot(maxCpu));
        }
        return builder.build();
    }

    /**
     * Creates a mock StatSnapshot with a list of mock StatRecords that have StatValues with the specified max values.
     *
     * @param maxCpu The max CPU value that the StatValue will return
     * @return A mock StatSnapshot
     */
    private Stats.StatSnapshot createMockStatSnapshot(float maxCpu) {
        return Stats.StatSnapshot.newBuilder()
                .addStatRecords(createMockStatRecord(maxCpu))
                .build();
    }

    /**
     * Creates a mock StatRecord that returns a mock StatValue that returns the specified max value when its getMax()
     * method is called.
     *
     * @param max The value to return when StatValue::getMax is called
     * @return A mock implementation of a StatRecord
     */
    private Stats.StatSnapshot.StatRecord createMockStatRecord(float max) {
        return Stats.StatSnapshot.StatRecord.newBuilder()
                .setUsed(createMockStatValue(max))
                .build();
    }

    /**
     * Creates a mock StatValue that returns the specified max value when its getMax() method is called.
     *
     * @param max The value to return when StatValue::getMax is called
     * @return A mock implementation of a StatValue
     */
    private Stats.StatSnapshot.StatRecord.StatValue createMockStatValue(float max) {
        return Stats.StatSnapshot.StatRecord.StatValue.newBuilder()
                .setMax(max)
                .build();
    }
}
