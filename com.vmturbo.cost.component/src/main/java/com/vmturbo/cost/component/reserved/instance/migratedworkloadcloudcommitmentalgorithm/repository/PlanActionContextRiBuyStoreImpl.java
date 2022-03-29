package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository;

import static com.vmturbo.cost.component.db.Tables.ACTION_CONTEXT_RI_BUY;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.jooq.DSLContext;

import com.vmturbo.cost.component.db.tables.pojos.ActionContextRiBuy;
import com.vmturbo.cost.component.db.tables.records.ActionContextRiBuyRecord;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore;

/**
 * PlanActionContextRiBuyStoreImpl.
 */
public class PlanActionContextRiBuyStoreImpl implements PlanActionContextRiBuyStore {

    private static final int NUM_HOURS_IN_A_WEEK = 168;

    /**
     * JOOQ DSL Context.
     */
    private final DSLContext dsl;

    /**
     * PlanActionContextRiBuyStoreImpl constructor.
     * @param dslContext the DSLContext.
     */
    public PlanActionContextRiBuyStoreImpl(DSLContext dslContext) {
        dsl = dslContext;
    }

    /**
     * Inserts a new action context RI buy record into the database.
     *
     * @param actionContextRiBuy The action context RI buy record to insert
     * @return The resultant ActionContextRiBuyRecord database record
     */
    @Override
    public ActionContextRiBuyRecord save(ActionContextRiBuy actionContextRiBuy) {
        // Build and configure a new ActionContextRiBuyRecord
        final ActionContextRiBuyRecord actionContextRiBuyRecord = dsl.newRecord(ACTION_CONTEXT_RI_BUY);
        actionContextRiBuyRecord.setActionId(actionContextRiBuy.getActionId());
        actionContextRiBuyRecord.setPlanId(actionContextRiBuy.getPlanId());
        actionContextRiBuyRecord.setCreateTime(LocalDateTime.now());
        actionContextRiBuyRecord.setTemplateType(actionContextRiBuy.getTemplateType());
        actionContextRiBuyRecord.setTemplateFamily(actionContextRiBuy.getTemplateFamily());
        actionContextRiBuyRecord.setDemandType(ActionContextRIBuyStore.DemandType.TYPICAL_DEMAND.ordinal());
        actionContextRiBuyRecord.setDatapointInterval(Duration.ofHours(1).toString());

        // Build a weekly demand list (only used to avoid runtime API errors)
        // Note: this is a hack, these values are meaningless, but needed by the action details API call
        List<Float> weeklyDemandList = new ArrayList<>();
        for (int hour = 0; hour < NUM_HOURS_IN_A_WEEK; hour++) {
            weeklyDemandList.add(0f);
        }

        final String demand = weeklyDemandList.toString()
                .substring(1, weeklyDemandList.toString().length() - 1);
        actionContextRiBuyRecord.setData(demand.getBytes());

        // Insert the record into the database
        dsl.batchInsert(actionContextRiBuyRecord).execute();

        // Return the record back to the caller
        return actionContextRiBuyRecord;
    }
}
