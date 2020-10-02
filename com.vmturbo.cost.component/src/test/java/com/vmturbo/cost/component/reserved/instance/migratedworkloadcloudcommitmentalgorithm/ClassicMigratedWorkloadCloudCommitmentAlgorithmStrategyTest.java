package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigratedWorkloadPlacement;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTOREST;

/**
 * Tests the ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy class.
 * Note that it loads a test Spring configuration from the ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTestConfig
 * class, which defines the ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy. It is gated by the
 * ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTest profile.
 */
@ActiveProfiles("ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTest")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTestConfig.class})
public class ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTest {

    private static final Logger logger = LogManager.getLogger(ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategyTest.class);

    /**
     * The MigratedWorkloadCloudCommitmentAlgorithmStrategy under test.
     */
    @Autowired
    private ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy migratedWorkloadCloudCommitmentAlgorithmStrategy;

    /**
     * Tests a VM with a set of max CPU values that fall below the threshold for identifying the VM as active.
     * No RI actions should be generated.
     */
    @Test
    public void testOneVMNoRIActionsGenerated() {
        // Build the original and new VMs
        TopologyEntityDTO originalVm = createVm(1L, 1000d, 500d, 0.5, true, true);
        TopologyEntityDTO newVm = createVm(1L, 8000d, 500d, 0.5, true, true);

        // Build the workload with the original and new VMs
        MigratedWorkloadPlacement workload = MigratedWorkloadPlacement.newBuilder()
                .setOriginalVirtualMachine(originalVm)
                .setVirtualMachine(newVm)
                .build();

        // Analyze the workloads
        List<Long> oids = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyzeWorkloads(Arrays.asList(workload));

        // Validate the results
        Assert.assertEquals("We should not recommend buying any RIs", 0, oids.size());
    }

    /**
     * Tests a VM with a set of max CPU values that are all above 20%, so an RI should be generated.
     */
    @Test
    public void testOneVMRIActionsGenerated() {
        // Build the original and new VMs
        TopologyEntityDTO originalVm = createVm(1L, 1000d, 500d, 0.5, true, true);
        TopologyEntityDTO newVm = createVm(1L, 2000d, 500d, 0.5, true, true);

        // Build the workload with the original and new VMs
        MigratedWorkloadPlacement workload = MigratedWorkloadPlacement.newBuilder()
                .setOriginalVirtualMachine(originalVm)
                .setVirtualMachine(newVm)
                .build();

        // Analyze the workloads
        List<Long> oids = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyzeWorkloads(Arrays.asList(workload));

        // Validate the results
        Assert.assertEquals("We should recommend buying RIs for 1 VM", 1, oids.size());
        Assert.assertEquals("We should receive OID 1", oids.get(0).longValue(), 1L);
    }

    /**
     * Tests two VMs that should both generate Buy RI actions.
     */
    @Test
    public void testTwoVMsRIActionsGenerated() {
        // Build the first workload
        TopologyEntityDTO originalVm1 = createVm(1L, 1000d, 500d, 0.5, true, true);
        TopologyEntityDTO newVm1 = createVm(1L, 2000d, 500d, 0.5, true, true);
        MigratedWorkloadPlacement workload1 = MigratedWorkloadPlacement.newBuilder()
                .setOriginalVirtualMachine(originalVm1)
                .setVirtualMachine(newVm1)
                .build();

        // Build the first workload
        TopologyEntityDTO originalVm2 = createVm(2L, 1000d, 500d, 0.5, true, true);
        TopologyEntityDTO newVm2 = createVm(2L, 2000d, 500d, 0.5, true, true);
        MigratedWorkloadPlacement workload2 = MigratedWorkloadPlacement.newBuilder()
                .setOriginalVirtualMachine(originalVm2)
                .setVirtualMachine(newVm2)
                .build();


        // Analyze the workloads
        List<Long> oids = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyzeWorkloads(Arrays.asList(workload1, workload2));

        // Validate the results
        Assert.assertEquals("We should recommend buying RIs for 2 VMs", 2, oids.size());
        Assert.assertEquals("We should receive OID 1", oids.get(0).longValue(), 1L);
        Assert.assertEquals("We should receivd OID 2", oids.get(1).longValue(), 2L);
    }

    /**
     * Tests two VMs, one that should generate a Buy RI action and one that should not.
     */
    @Test
    public void testTwoVMsOneRIActionGenerated() {
        // Build the first workload
        TopologyEntityDTO originalVm1 = createVm(1L, 1000d, 500d, 0.5, true, true);
        TopologyEntityDTO newVm1 = createVm(1L, 2000d, 500d, 0.5, true, true);
        MigratedWorkloadPlacement workload1 = MigratedWorkloadPlacement.newBuilder()
                .setOriginalVirtualMachine(originalVm1)
                .setVirtualMachine(newVm1)
                .build();

        // Build the first workload
        TopologyEntityDTO originalVm2 = createVm(2L, 1000d, 500d, 0.5, true, true);
        TopologyEntityDTO newVm2 = createVm(2L, 8000d, 500d, 0.5, true, true);
        MigratedWorkloadPlacement workload2 = MigratedWorkloadPlacement.newBuilder()
                .setOriginalVirtualMachine(originalVm2)
                .setVirtualMachine(newVm2)
                .build();


        // Analyze the workloads
        List<Long> oids = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyzeWorkloads(Arrays.asList(workload1, workload2));

        // Validate the results
        Assert.assertEquals("We should only recommend buying RIs for 1 VM", 1, oids.size());
        Assert.assertEquals("We should receive OID 1", oids.get(0).longValue(), 1L);
    }

    /**
     * Tests the percentile calculation that returns a valid value.
     */
    @Test
    public void testPercentileCalculation() {
        // Build the original and new VMs
        TopologyEntityDTO originalVm = createVm(1L, 1000d, 500d, 0.5, true, true);
        TopologyEntityDTO newVm = createVm(1L, 2000d, 500d, 0.5, true, true);

        // Build the workload with the original and new VMs
        MigratedWorkloadPlacement workload = MigratedWorkloadPlacement.newBuilder()
                .setOriginalVirtualMachine(originalVm)
                .setVirtualMachine(newVm)
                .build();

        // Calculate the percentile
        Optional<Double> percentile = migratedWorkloadCloudCommitmentAlgorithmStrategy.computeProjectedPercentile(workload);

        // Validate the percentile
        Assert.assertTrue("We should have found a percentile", percentile.isPresent());
        Assert.assertEquals(0.25d, percentile.get(), 0.001);
    }

    /**
     * Tests the percentile calculation that returns an empty result due to no percentile.
     */
    @Test
    public void testPercentileCalculationNoResultMissingPercentile() {
        // Build the original and new VMs
        TopologyEntityDTO originalVm = createVm(1L, 1000d, 500d, 0.5, false, true);
        TopologyEntityDTO newVm = createVm(1L, 2000d, 500d, 0.5, true, true);

        // Build the workload with the original and new VMs
        MigratedWorkloadPlacement workload = MigratedWorkloadPlacement.newBuilder()
                .setOriginalVirtualMachine(originalVm)
                .setVirtualMachine(newVm)
                .build();

        // Calculate the percentile
        Optional<Double> percentile = migratedWorkloadCloudCommitmentAlgorithmStrategy.computeProjectedPercentile(workload);

        // Validate the percentile
        Assert.assertFalse("We should not have found a percentile", percentile.isPresent());
    }

    /**
     * Tests the percentile calculation that returns an empty result due to no percentile.
     */
    @Test
    public void testPercentileCalculationNoResultMissingVcpuCommodity() {
        // Build the original and new VMs
        TopologyEntityDTO originalVm = createVm(1L, 1000d, 500d, 0.5, false, false);
        TopologyEntityDTO newVm = createVm(1L, 2000d, 500d, 0.5, true, true);

        // Build the workload with the original and new VMs
        MigratedWorkloadPlacement workload = MigratedWorkloadPlacement.newBuilder()
                .setOriginalVirtualMachine(originalVm)
                .setVirtualMachine(newVm)
                .build();

        // Calculate the percentile
        Optional<Double> percentile = migratedWorkloadCloudCommitmentAlgorithmStrategy.computeProjectedPercentile(workload);

        // Validate the percentile
        Assert.assertFalse("We should not have found a percentile", percentile.isPresent());
    }

    /**
     * Helper method that builds a VM with a VCPU commodity (optional) with the specified values.
     *
     * @param oid               The OID of the VM
     * @param capacity          The VCPU capacity
     * @param used              The VCPU used
     * @param percentile        The VCPU percentile
     * @param includePercentile Should it include a percentile value?
     * @param includeCommodity  Should it include the VCPU commodity at all?
     * @return A configured TopologyEntityDTO for the VM
     */
    private TopologyEntityDTO createVm(Long oid, Double capacity, Double used, Double percentile, boolean includePercentile, boolean includeCommodity) {
        TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder();
        builder.setOid(oid);
        builder.setEntityType(CommonDTOREST.EntityDTO.EntityType.VIRTUAL_MACHINE.getValue());
        if (includeCommodity) {
            // If we should include the VCPU commodity, build it
            TopologyDTO.CommoditySoldDTO.Builder commodityBuilder = TopologyDTO.CommoditySoldDTO.newBuilder();
            commodityBuilder.setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(26).build());
            commodityBuilder.setCapacity(capacity);
            commodityBuilder.setUsed(used);

            TopologyDTO.HistoricalValues.Builder historicalValuesBuilder = TopologyDTO.HistoricalValues.newBuilder();
            if (includePercentile) {
                // Add the percentile value
                historicalValuesBuilder.setPercentile(percentile);
            }

            // Add the historical used value
            commodityBuilder.setHistoricalUsed(historicalValuesBuilder.build());

            // Add the constructed commodity
            builder.addCommoditySoldList(commodityBuilder.build());
        }

        // Return the built VM
        return builder.build();
    }
}
