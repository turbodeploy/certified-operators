package com.vmturbo.action.orchestrator.store.identity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.RecommendationIdentityRecord;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;

/**
 * Identity store for market recommendations.
 */
public class RecommendationIdentityStore implements IdentityDataStore<ActionInfoModel> {

    private final DSLContext context;
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Constructs the store.
     *
     * @param context DB context to use.
     */
    public RecommendationIdentityStore(@Nonnull DSLContext context) {
        this.context = Objects.requireNonNull(context);
    }

    @Override
    @Nonnull
    public Map<ActionInfoModel, Long> fetchOids(@Nonnull Collection<ActionInfoModel> models) {
        if (models.isEmpty()) {
            return Collections.emptyMap();
        }
        final Condition filter = getCondition(models);
        final List<RecommendationIdentityRecord> records =
                context.selectFrom(Tables.RECOMMENDATION_IDENTITY).where(filter).fetch();
        final Map<ActionInfoModel, Long> result = new HashMap<>(records.size());
        for (RecommendationIdentityRecord record : records) {
            final ActionTypeCase actionType = ActionTypeCase.forNumber(record.getActionType());
            if (actionType == null) {
                logger.error(
                        "Malformed action identity record found for OID {} containing unknown action type {}",
                        record.getId(), record.getActionType());
            }
            final ActionInfoModel model = new ActionInfoModel(actionType, record.getTargetId(),
                    record.getActionDetails());
            result.put(model, record.getId());
        }
        return result;
    }

    private Condition getCondition(@Nonnull Collection<ActionInfoModel> actions) {
        Condition result = DSL.falseCondition();
        for (ActionInfoModel action : actions) {
            final Condition actionCondition = getModelCondition(action);
            result = result.or(actionCondition);
        }
        return result;
    }

    @Nonnull
    private Condition getModelCondition(@Nonnull ActionInfoModel model) {
        final Condition result =
                Tables.RECOMMENDATION_IDENTITY.ACTION_TYPE.eq(model.getActionType().getNumber())
                        .and(Tables.RECOMMENDATION_IDENTITY.TARGET_ID.eq(model.getTargetId()));
        if (model.getDetails().isPresent()) {
            return result.and(
                    Tables.RECOMMENDATION_IDENTITY.ACTION_DETAILS.eq(model.getDetails().get()));
        } else {
            return result;
        }
    }

    @Override
    public void persistModels(@Nonnull Map<ActionInfoModel, Long> models) {
        final List<RecommendationIdentityRecord> records = new ArrayList<>(models.size());
        for (Entry<ActionInfoModel, Long> entry : models.entrySet()) {
            final ActionInfoModel model = entry.getKey();
            final RecommendationIdentityRecord record =
                    new RecommendationIdentityRecord(entry.getValue(),
                            model.getActionType().getNumber(), model.getTargetId(),
                            model.getDetails().orElse(null));
            records.add(record);
        }
        context.batchInsert(records).execute();
    }
}
