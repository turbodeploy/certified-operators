package com.vmturbo.action.orchestrator.stats;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup.MgmtUnitSubgroupKey;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;

/**
 * A wrapper class to help aggregate stats for all actions affecting a particular
 * {@link ManagementUnitGroupKey} and {@link ActionGroupKey} pair.
 */
@NotThreadSafe
public class ActionStat {

    /**
     * The number of actions for the management unit and action group tracked by
     * this {@link ActionStat}.
     */
    private int actionCount = 0;

    private double savings = 0;

    private double investment = 0;

    /**
     * The entities involved in actions tracked by this stat.
     *
     * We use a set to avoid double-counting entities - e.g. if a host is the destination of multiple
     * moves with the same {@link MgmtUnitSubgroupKey} and {@link ActionGroupKey}.
     */
    private Set<Long> involvedEntities = new HashSet<>();

    /**
     * Record a particular action affecting the management unit and action group tracked by this
     * {@link ActionStat}. This method performs no checks on its input - it's the caller's
     * responsibility to avoid double-counting, and to filter out recommendations that don't match
     * the {@link MgmtUnitSubgroupKey} and {@link ActionGroupKey} associated with this {@link ActionStat}.
     *
     * @param recommendation The action recommendation.
     * @param involvedEntities The entities to count towards this {@link ActionStat}. This will be
     *    a subset of the entities involved in the action. We pass it separately because not all
     *    involved entities matter for a particular management unit and action group. e.g. if a
     *    VM in cluster-scope has a storage move, we count the VM, but not the storages.
     */
    public void recordAction(@Nonnull final ActionDTO.Action recommendation,
                             @Nonnull final Collection<ActionEntity> involvedEntities) {
        this.actionCount++;
        if (recommendation.getSavingsPerHour().getAmount() >= 0) {
            this.savings += recommendation.getSavingsPerHour().getAmount();
        } else {
            // Subtract, because savings is negative.
            this.investment -= recommendation.getSavingsPerHour().getAmount();
        }
        involvedEntities.forEach(entity -> this.involvedEntities.add(entity.getId()));
    }

    /**
     * Add the properties of this {@link ActionStat} to a {@link ActionStatsLatestRecord} to be
     * saved to the database.
     *
     * @param record The record. This method will modify this input.
     */
    public void addToRecord(@Nonnull final ActionStatsLatestRecord record) {
        record.setTotalActionCount(actionCount);
        record.setTotalEntityCount(involvedEntities.size());
        record.setTotalSavings(BigDecimal.valueOf(savings));
        record.setTotalInvestment(BigDecimal.valueOf(investment));
    }
}
