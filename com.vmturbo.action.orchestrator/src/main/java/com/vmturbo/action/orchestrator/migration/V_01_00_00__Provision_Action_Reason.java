package com.vmturbo.action.orchestrator.migration;


import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.MarketActionRecord;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.components.common.migration.AbstractMigration;
import com.vmturbo.components.common.migration.Migration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Migrating old provision actions to the new format.
 */
public class V_01_00_00__Provision_Action_Reason extends AbstractMigration {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;

    public V_01_00_00__Provision_Action_Reason(@Nonnull DSLContext dslContext) {
        this.dslContext = Objects.requireNonNull(dslContext);
    }

    @Override
    public MigrationProgressInfo doStartMigration() {
        final List<MarketActionRecord> recordsToUpdate = new ArrayList<>();

        try {
            dslContext.transaction(config -> {
                final DSLContext transactionContext = DSL.using(config);
                transactionContext
                        .selectFrom(Tables.MARKET_ACTION)
                        .fetch().forEach(actionRecord -> {
                    if (handleRecord(actionRecord)) {
                        recordsToUpdate.add(actionRecord);
                    }
                });
                logger.info("Found {} actions to update",
                        recordsToUpdate.size());
                long errorsUpdating =
                        Arrays.stream(transactionContext.batchUpdate(recordsToUpdate).execute())
                                .filter(numRows -> numRows != 1)
                                .count();
                if (errorsUpdating > 0) {
                    throw new IllegalStateException("Failed to update " + errorsUpdating + " actions");
                }
            });
        } catch (DataAccessException e) {
            logger.error("Failed to update the database", e);
            return migrationFailed(e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error occurred", e);
            return migrationFailed(e.getMessage());
        }

        return migrationSucceeded();
    }

    /**
     * Handles a DB record for a ProvisionBySupply action.
     *
     * @param action The mutable DB record.
     *              If the record needs to be updated,
     *              a setter will be called on this object.
     * @return true if the record is to be updated and
     *         false if it is to be deleted
     */
    private boolean handleRecord(@Nonnull MarketActionRecord action) {
        try {
            // parse the blob into a Action protobuf object
            // check if this is a provision action
            if (action.getRecommendation() == null) {
                return false;
            }
            if (!action.getRecommendation().getExplanation().hasProvision()) {
                return false;
            }
            // check if this is a provisionBySupply action
            if (!action.getRecommendation().getExplanation().getProvision().hasProvisionBySupplyExplanation()) {
                return false;
            }
            ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation exp =
                    action.getRecommendation().getExplanation().getProvision().getProvisionBySupplyExplanation();
            if (exp.hasMostExpensiveCommodity()) {
                TopologyDTO.CommodityType commType = TopologyDTO.CommodityType.newBuilder().setType(exp.getMostExpensiveCommodity()).build();
                ProvisionBySupplyExplanation newExp = ProvisionBySupplyExplanation.newBuilder(exp).setMostExpensiveCommodityInfo(
                        Explanation.ReasonCommodity.newBuilder().setCommodityType(commType).build()).build();
                ProvisionExplanation provision = ProvisionExplanation.newBuilder(action.getRecommendation().getExplanation().getProvision())
                        .setProvisionBySupplyExplanation(newExp).build();
                Explanation explanation = Explanation.newBuilder(action.getRecommendation().getExplanation()).setProvision(provision).build();
                Action recommendation = Action.newBuilder(action.getRecommendation()).setExplanation(explanation).build();
                action.setRecommendation(recommendation);
            }
        } catch (IllegalArgumentException e) {
            logger.error("Action data from action " + action.getDescription() + " cannot be converted", e);
            return false;
        }
        return true;
    }

}
