package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigratedWorkloadPlacement;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.cost.component.history.HistoricalStatsService;

/**
 * MigratedWorkloadCloudCommitmentAlgorithmStrategy that replicates the strategy used in the classic OpsManager.
 */
@Service
public class ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy implements MigratedWorkloadCloudCommitmentAlgorithmStrategy {
    private static final Logger logger = LogManager.getLogger(ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy.class);

    /**
     * The historical service, from which we retrieve historical VM vCPU usage.
     */
    private HistoricalStatsService historicalStatsService;

    /**
     * Create a new ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy.
     *
     * @param historicalStatsService    The historical status service that will be used to retrieve historical VCPU metrics
     */
    public ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy(HistoricalStatsService historicalStatsService) {
        this.historicalStatsService = historicalStatsService;
    }

    /**
     * The number of historical days to analyze VM usage.
     */
    @Value("${migratedWorkflowCloudCommitmentAnalysis.numberOfHistoricalDays:21}")
    private int numberOfHistoricalDays;

    /**
     * The vCPU usage threshold: the VM is considered "active" for a day if its vCPU -> Used -> Max is greater than
     * this threshold. The default value is 20%.
     */
    @Value("${migratedWorkflowCloudCommitmentAnalysis.commodityThreshold:20}")
    private int commodityThreshold;

    /**
     * The percentage of time that this VM must be "active" in the specified number of historical days for us to
     * recommend buying an RI. The default value is 80%.
     */
    @Value("${migratedWorkflowCloudCommitmentAnalysis.activeDaysThreshold:80}")
    private int activeDaysThreshold;

    /**
     * Performs the analysis of our input data and generates Buy RI recommendations.
     *
     * @param migratedWorkloads The workloads that are being migrated as part of a migrate to cloud plan
     * @return A list of Buy RI actions for these workloads
     */
    @Override
    public List<ActionDTO.Action> analyze(List<MigratedWorkloadPlacement> migratedWorkloads) {
        logger.info("Start executing ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy");

        // Extract the list of OIDs to analyze
        List<Long> oids = migratedWorkloads.stream()
                .map(workload -> workload.getVirtualMachine().getOid())
                .collect(Collectors.toList());

        // Retrieve historical statistics for our VMs
        List<Stats.EntityStats> stats = historicalStatsService.getHistoricalStats(oids, Arrays.asList("VCPU"), numberOfHistoricalDays);

        // Determine the minimum number of days that a VM must be active
        int minimumNumberOfDaysActive = (int) ((double) activeDaysThreshold / 100 * numberOfHistoricalDays);

        // Capture the OIDs of the VMs for which we want to buy an RI
        List<Long> oidsForWhichToBuyRIs = new ArrayList<>();

        // Iterate over the stats for each VM
        for (Stats.EntityStats stat : stats) {
            // Count the number of "active" days for this VM
            int activeDays = 0;

            // Each EntityStats will have one snapshot for each day from the history service
            for (Stats.StatSnapshot snapshot : stat.getStatSnapshotsList()) {
                List<Stats.StatSnapshot.StatRecord> records = snapshot.getStatRecordsList();
                if (CollectionUtils.isNotEmpty(records)) {
                    // Each snapshot should have a single StatRecord
                    Stats.StatSnapshot.StatRecord.StatValue usedValue = records.get(0).getUsed();
                    if (usedValue != null) {
                        // Compare the used -> max value to our commodity threshold
                        if (usedValue.getMax() > commodityThreshold) {
                            activeDays++;
                        }
                    }
                }
            }

            // If the number of active days is above our minimum then add it to our list
            logger.info("{} - number of days active: {}, minimum days required: {}", stat.getOid(), activeDays, minimumNumberOfDaysActive);
            if (activeDays > minimumNumberOfDaysActive) {
                oidsForWhichToBuyRIs.add(stat.getOid());
            }
        }

        logger.info("Buy RIs for OIDs: {}", oidsForWhichToBuyRIs);
        oidsForWhichToBuyRIs.forEach(oid -> {
            Optional<MigratedWorkloadPlacement> migratedWorkloadPlacement = migratedWorkloads.stream()
                    .filter(workload -> workload.getVirtualMachine().getOid() == oid)
                    .findFirst();

            if (migratedWorkloadPlacement.isPresent()) {
                logger.info("Buy RI for VM {} - Compute Tier: {}, Region: {}",
                        migratedWorkloadPlacement.get().getVirtualMachine().getDisplayName(),
                        migratedWorkloadPlacement.get().getComputeTier().getDisplayName(),
                        migratedWorkloadPlacement.get().getRegion().getDisplayName());
            }
        });

        logger.info("Finished executing ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy");
        return new ArrayList<>();
    }
}
