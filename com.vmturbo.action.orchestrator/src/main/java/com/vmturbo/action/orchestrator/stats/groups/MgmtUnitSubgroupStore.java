package com.vmturbo.action.orchestrator.stats.groups;

import static com.vmturbo.action.orchestrator.db.Tables.MGMT_UNIT_SUBGROUP;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.google.common.collect.Maps;

import com.vmturbo.action.orchestrator.db.tables.records.MgmtUnitSubgroupRecord;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup.MgmtUnitSubgroupKey;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;

/**
 * Responsible for storing and retrieving {@link MgmtUnitSubgroup}s to/from the underlying database.
 */
public class MgmtUnitSubgroupStore {

    private static final Logger logger = LogManager.getLogger();

    private static final short UNSET_ENUM_VALUE = -1;

    private final DSLContext dsl;

    public MgmtUnitSubgroupStore(@Nonnull final DSLContext dsl) {
        this.dsl = Objects.requireNonNull(dsl);
    }

    /**
     * Ensure that a set of {@link MgmtUnitSubgroupKey}s exist in the database, and return the
     * {@link MgmtUnitSubgroup} the keys represent. If an {@link MgmtUnitSubgroupKey} does not match an
     * existing {@link MgmtUnitSubgroup}, this method will create the {@link MgmtUnitSubgroup} record.
     *
     * @param keys A set of {@link MgmtUnitSubgroupKey}s.
     * @return key -> {@link MgmtUnitSubgroup}. Each key in the input should have an associated
     *         {@link MgmtUnitSubgroup}.
     */
    @Nonnull
    public Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> ensureExist(@Nonnull final Set<MgmtUnitSubgroupKey> keys) {
        if (keys.isEmpty()) {
            return Collections.emptyMap();
        }

        return dsl.transactionResult(transactionContext -> {
            final DSLContext transactionDsl = DSL.using(transactionContext);

            final int[] inserted = transactionDsl.batch(keys.stream()
                    .map(key -> transactionDsl.insertInto(MGMT_UNIT_SUBGROUP)
                        .set(keyToRecord(key))
                        .onDuplicateKeyIgnore())
                    .collect(Collectors.toList()))
                .execute();

            final int insertedSum = IntStream.of(inserted).sum();
            if (insertedSum > 0) {
                logger.info("Inserted {} action groups.", insertedSum);
            }

            final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> allExistingMgmtUnits =
                transactionDsl.selectFrom(MGMT_UNIT_SUBGROUP)
                    .fetch()
                    .stream()
                    .map(this::recordToGroup)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toMap(MgmtUnitSubgroup::key, Function.identity()));

            logger.debug("A total of {} mgmt unit subgroups now exist.", allExistingMgmtUnits.size());

            return Maps.filterKeys(allExistingMgmtUnits, keys::contains);
        });
    }

    @Nonnull
    private Optional<MgmtUnitSubgroup> recordToGroup(@Nonnull final MgmtUnitSubgroupRecord record) {
        final ImmutableMgmtUnitSubgroupKey.Builder keyBuilder = ImmutableMgmtUnitSubgroupKey.builder()
            .mgmtUnitId(record.getMgmtUnitId());
        if (record.getEntityType() != null && record.getEntityType() != UNSET_ENUM_VALUE) {
            keyBuilder.entityType(record.getEntityType());
        }
        if (record.getEnvironmentType() != null) {
            EnvironmentType environmentType = EnvironmentType.forNumber(record.getEnvironmentType());
            if (environmentType != null) {
                keyBuilder.environmentType(environmentType);
            } else if (record.getEnvironmentType() != UNSET_ENUM_VALUE) {
                logger.error("Unrecognized environment type in database record: {}",
                    record.getEnvironmentType());
            }
        }

        try {
            return Optional.of(ImmutableMgmtUnitSubgroup.builder()
                    .id(record.getId())
                    .key(keyBuilder.build())
                    .build());
        } catch (IllegalStateException e) {
            logger.error("Failed to build subgroup out of database record. Error: {}",
                    e.getLocalizedMessage());
            return Optional.empty();
        }
    }

    @Nonnull
    private MgmtUnitSubgroupRecord keyToRecord(@Nonnull final MgmtUnitSubgroupKey key) {
        MgmtUnitSubgroupRecord record = new MgmtUnitSubgroupRecord();
        // Leave record ID unset - database auto-increment takes care of ID assignment.

        record.setMgmtUnitId(key.mgmtUnitId());
        if (key.entityType().isPresent()) {
            record.setEntityType(key.entityType().get().shortValue());
        } else {
            record.setEntityType(UNSET_ENUM_VALUE);
        }

        if (key.environmentType().isPresent()) {
            record.setEnvironmentType((short)key.environmentType().get().getNumber());
        } else {
            record.setEnvironmentType(UNSET_ENUM_VALUE);
        }
        return record;
    }

}
