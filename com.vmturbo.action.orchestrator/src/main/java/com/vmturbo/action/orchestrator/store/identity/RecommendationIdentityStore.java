package com.vmturbo.action.orchestrator.store.identity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.xml.bind.DatatypeConverter;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.RecommendationIdentityRecord;

/**
 * Identity store for market recommendations.
 */
public class RecommendationIdentityStore implements IdentityDataStore<ActionInfoModel> {

    private final int modelsChunkSize;
    private final DSLContext context;
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Constructs the store.
     *
     * @param context DB context to use.
     * @param modelsChunkSize a chunk size to split models requests to the DB. It turned out
     *         that extra large requests are processed unstable in the DB, so we split the requests
     *         to certain amount of records requestsd
     */
    public RecommendationIdentityStore(@Nonnull DSLContext context, int modelsChunkSize) {
        this.context = Objects.requireNonNull(context);
        if (modelsChunkSize < 1) {
            throw new IllegalArgumentException(
                    "modelsChunkSize must be a positive value. Got " + modelsChunkSize);
        }
        this.modelsChunkSize = modelsChunkSize;
    }

    @Override
    @Nonnull
    public Map<ActionInfoModel, Long> fetchOids(@Nonnull Collection<ActionInfoModel> models) {
        if (models.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, ActionInfoModel> hashToAction = models
            .stream()
            .collect(Collectors.toMap(ActionInfoModel::getActionHexHash, Function.identity(),
                (a, b) -> a));

        final Collection<RecommendationIdentityRecord> records = retrieveRecords(models);
        final Map<ActionInfoModel, Long> result = new HashMap<>(records.size());

        for (RecommendationIdentityRecord record : records) {
            result.put(hashToAction.get(DatatypeConverter.printHexBinary(record.getActionHash())),
                record.getId());
        }
        logger.debug("Successfully fetched {} recommendation identities from the DB", result::size);
        return result;
    }

    /**
     * Retrieves recommendation identity records for the specified action models. If the incoming
     * set is too large, this method will also chunk the requests.
     *
     * @param models models to query
     * @return list of records
     */
    @Nonnull
    private Collection<RecommendationIdentityRecord> retrieveRecords(
            @Nonnull Collection<ActionInfoModel> models) {
        final Collection<RecommendationIdentityRecord> result = new ArrayList<>(models.size());
        for (Collection<ActionInfoModel> modelsChunk : Iterables.partition(models,
                modelsChunkSize)) {
            logger.debug("Retrieving next {} recommendation identity records",
                    modelsChunk.size());
            final Condition filter = getCondition(modelsChunk);
            final List<RecommendationIdentityRecord> records = context.selectFrom(
                    Tables.RECOMMENDATION_IDENTITY).where(filter).fetch();
            result.addAll(records);
        }
        logger.debug("Fetched {} parent records for {} requested actions",
                result.size(), models.size());
        return Collections.unmodifiableCollection(result);
    }

    @Nonnull
    private Table<Integer, Long, Collection<ActionInfoModel>> groupActionModels(
            @Nonnull Collection<ActionInfoModel> complexRequests) {
        final Table<Integer, Long, Collection<ActionInfoModel>> requestedActions =
                HashBasedTable.create();
        for (ActionInfoModel model : complexRequests) {
            if (model.getAdditionalDetails().isPresent()) {
                Collection<ActionInfoModel> models = requestedActions.get(
                        model.getActionType().getNumber(), model.getTargetId());
                if (models == null) {
                    models = new ArrayList<>();
                    requestedActions.put(model.getActionType().getNumber(), model.getTargetId(),
                            models);
                }
                models.add(model);
            }
        }
        return requestedActions;
    }

    private Condition getCondition(@Nonnull Collection<ActionInfoModel> actions) {
        Condition result = DSL.falseCondition();
        for (ActionInfoModel action : actions) {
            final Condition actionCondition =
                Tables.RECOMMENDATION_IDENTITY.ACTION_HASH.eq(action.getActionHash());
            result = result.or(actionCondition);
        }
        return result;
    }

    @Override
    public void persistModels(@Nonnull Map<ActionInfoModel, Long> models) {
        final List<RecommendationIdentityRecord> records = new ArrayList<>(models.size());
        for (Entry<ActionInfoModel, Long> entry : models.entrySet()) {
            final ActionInfoModel model = entry.getKey();
            final long recommendationOid = entry.getValue();
            final RecommendationIdentityRecord record = new RecommendationIdentityRecord(
                    recommendationOid, model.getActionHash());
            records.add(record);
        }
        context.batchInsert(records).execute();
    }
}
