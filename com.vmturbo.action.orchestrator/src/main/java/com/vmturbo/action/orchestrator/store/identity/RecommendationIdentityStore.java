package com.vmturbo.action.orchestrator.store.identity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Collections2;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.RecommendationIdentityDetailsRecord;
import com.vmturbo.action.orchestrator.db.tables.records.RecommendationIdentityRecord;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;

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
        final Set<ActionInfoModel> modelsSet = new HashSet<>(models);
        final Collection<RecommendationIdentityRecord> records = retrieveRecords(models);
        final Map<ActionInfoModel, Long> result = new HashMap<>(records.size());
        final Collection<RecommendationIdentityRecord> complexRecords = new ArrayList<>(
                models.size());
        for (RecommendationIdentityRecord record : records) {
            final ActionTypeCase actionType = ActionTypeCase.forNumber(record.getActionType());
            if (actionType == null) {
                logger.error(
                        "Malformed action identity record found for OID {} containing unknown action type {}",
                        record.getId(), record.getActionType());
            } else {
                final ActionInfoModel model = new ActionInfoModel(actionType, record.getTargetId(),
                        record.getActionDetails(), null);
                if (modelsSet.remove(model)) {
                    result.put(model, record.getId());
                } else {
                    complexRecords.add(record);
                }
            }
        }
        result.putAll(fetchComplexRecords(modelsSet, complexRecords));
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

    @Nonnull
    private Map<Long, Set<String>> getActionDetails(
            @Nonnull Collection<RecommendationIdentityRecord> recommRecords) {
        final Collection<Long> recordIds = Collections2.transform(recommRecords,
                RecommendationIdentityRecord::getId);
        logger.debug("Requesting additional info for {} recommendation identity records",
                recommRecords);
        final Map<Long, Set<String>> result = new HashMap<>(recommRecords.size() * 2);
        for (Collection<Long> idsChunk: Iterables.partition(recordIds, modelsChunkSize)) {
            logger.debug("Fetching additional {} identity data records", modelsChunkSize);
            final Map<Long, Set<String>> records = context.selectFrom(
                    Tables.RECOMMENDATION_IDENTITY_DETAILS)
                    .where(Tables.RECOMMENDATION_IDENTITY_DETAILS.RECOMMENDATION_ID.in(idsChunk))
                    .fetch()
                    .stream()
                    .collect(Collectors.groupingBy(
                            RecommendationIdentityDetailsRecord::getRecommendationId,
                            Collectors.mapping(RecommendationIdentityDetailsRecord::getDetail,
                                    Collectors.toSet())));
            result.putAll(records);
        }
        logger.debug("Fetched {} detail records for {} identity records",
                () -> result.values()
                        .stream()
                        .map(Collection::size)
                        .reduce((a, b) -> a + b)
                        .orElse(0),
                () -> result.keySet().size());
        return result;
    }

    @Nonnull
    private Map<ActionInfoModel, Long> fetchComplexRecords(
            @Nonnull Collection<ActionInfoModel> complexRequests,
            @Nonnull Collection<RecommendationIdentityRecord> complexReqRecords) {
        final Table<Integer, Long, Collection<ActionInfoModel>> requestedActions =
                groupActionModels(complexRequests);
        final Map<ActionInfoModel, Long> result = new HashMap<>(complexReqRecords.size());
        final Map<Long, Set<String>> records = getActionDetails(complexReqRecords);
        for (RecommendationIdentityRecord record: complexReqRecords) {
            final long recommendationId = record.getId();
            final Set<String> detailsInDb = records.getOrDefault(recommendationId, Collections.emptySet());
            for (ActionInfoModel model: getModelForRecord(requestedActions, record)) {
                if (model.getAdditionalDetails().equals(Optional.of(detailsInDb))) {
                    result.put(model, recommendationId);
                    break;
                }
            }
        }
        return result;
    }

    @Nonnull
    private Collection<ActionInfoModel> getModelForRecord(
            Table<Integer, Long, Collection<ActionInfoModel>> requestedActions,
            RecommendationIdentityRecord recommendationRecord) {
        final Collection<ActionInfoModel> models = requestedActions.get(
                recommendationRecord.getActionType(), recommendationRecord.getTargetId());
        if (models == null) {
            return Collections.emptyList();
        } else {
            return models.stream().filter(mdl -> mdl.getDetails()
                    .equals(Optional.ofNullable(recommendationRecord.getActionDetails()))).collect(
                    Collectors.toList());
        }
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
        final Condition result = Tables.RECOMMENDATION_IDENTITY.ACTION_TYPE.eq(
                model.getActionType().getNumber()).and(
                Tables.RECOMMENDATION_IDENTITY.TARGET_ID.eq(model.getTargetId()));
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
        final List<RecommendationIdentityDetailsRecord> detailRecords = new ArrayList<>(models.size());
        for (Entry<ActionInfoModel, Long> entry : models.entrySet()) {
            final ActionInfoModel model = entry.getKey();
            final long recommendationOid = entry.getValue();
            final RecommendationIdentityRecord record = new RecommendationIdentityRecord(
                    recommendationOid, model.getActionType().getNumber(), model.getTargetId(),
                    model.getDetails().orElse(null));
            records.add(record);
            model.getAdditionalDetails().ifPresent(details -> {
                for (String detail : details) {
                    final RecommendationIdentityDetailsRecord detailRecord =
                            new RecommendationIdentityDetailsRecord(recommendationOid, detail);
                    detailRecords.add(detailRecord);
                }
            });
        }
        context.batchInsert(records).execute();
        context.batchInsert(detailRecords).execute();
    }
}
