package com.vmturbo.plan.orchestrator.plan.export;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus.PlanExportState;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * Simulate execution of a plan export. Temporary to enable UI development and testing
 * before the actual export is implemented.
 */
public class PlanExportRunner implements Runnable {
    private final Logger logger = LogManager.getLogger();

    private final PlanExportRpcService exportService;
    private final PlanInstance plan;
    private final PlanDestination destination;

    /**
     * Create a runnable that will simulate plan export.
     *
     * @param exportService the export service, which will receive updates.
     * @param plan the plan to export
     * @param destination the destination to which the plan should be exported.
     */
    public PlanExportRunner(@Nonnull final PlanExportRpcService exportService,
                            @Nonnull final PlanInstance plan,
                            @Nonnull final PlanDestination destination) {
        this.exportService = exportService;
        this.plan = plan;
        this.destination = destination;
    }

    @Override
    public void run() {
        boolean fail = plan.getScenario().getScenarioInfo().getName().contains("ALLOCATION");

        for (int i = 0; i <= (fail ? 21 : 50); i++) {
            try {
                exportService.updateDestination(destination.getOid(),
                    PlanExportStatus.newBuilder()
                        .setState(PlanExportState.IN_PROGRESS)
                        .setDescription("Doing step " + i + " of 50")
                        .setProgress(i * 2)
                        .build(),
                    null);
            } catch (NoSuchObjectException ex) {
                logger.error("updateDestination", ex);
            } catch (IntegrityException ex) {
                logger.error("updateDestination", ex);
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            if (fail) {
                exportService.updateDestination(destination.getOid(),
                    PlanExportStatus.newBuilder()
                        .setState(PlanExportState.FAILED)
                        .setDescription("Simulated failure due to ALLOCATION plan")
                        .setProgress(42)
                        .build(),
                    null);
            } else {
                exportService.updateDestination(destination.getOid(),
                    PlanExportStatus.newBuilder()
                        .setState(PlanExportState.SUCCEEDED)
                        .setDescription("Export succeeded.")
                        .setProgress(100)
                        .build(),
                    null);
            }
        } catch (NoSuchObjectException ex) {
            logger.error("updateDestination", ex);
        } catch (IntegrityException ex) {
            logger.error("updateDestination", ex);
        }
    }
}
