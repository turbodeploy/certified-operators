package com.vmturbo.cost.component.billedcosts;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;

import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.MultiStoreDiagnosable;
import com.vmturbo.cost.component.TableDiagsRestorable;
import com.vmturbo.cost.component.billed.cost.CloudCostDiags;
import com.vmturbo.cost.component.billed.cost.tag.TagGroupIdentity;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.CostTagGrouping;
import com.vmturbo.cost.component.db.tables.records.CostTagGroupingRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * An object of this type contains data operations for the Cost tag grouping tables.
 */
public class TagGroupStore implements MultiStoreDiagnosable, CloudCostDiags {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;

    private final Map<Long, TagGroupIdentity> tagGroupIdentityCache = new ConcurrentHashMap<>();

    private final CostTagGroupingDiagsHelper costTagGroupingDiagsHelper;

    private boolean exportCloudCostDiags = true;

    /**
     * Creates an instance of TagGroupStore.
     *
     * @param dslContext instance for executing queries.
     */
    public TagGroupStore(@Nonnull final DSLContext dslContext) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.costTagGroupingDiagsHelper = new CostTagGroupingDiagsHelper(dslContext);
    }

    /**
     * Retrieves all tag group records from the cost tag grouping table.
     *
     * @return all tag group records.
     * @throws DbException on encountering error while executing select query.
     */
    public List<TagGroupIdentity> retrieveAllTagGroups() throws DbException {
        try {
            return streamTagGroupIdentities(Collections.emptySet()).collect(ImmutableList.toImmutableList());
        } catch (DataAccessException ex) {
            throw new DbException("Exception while retrieving tag groups.", ex.getCause());
        }
    }

    /**
     * Retrieves the tag groups associated with each requested tag group ID. If a tag group cannot be
     * found for a given ID, it will be skipped.
     * @param tagGroupIds The tag group IDs.
     * @return The tag group identities, indexed by each tag group ID.
     */
    @Nonnull
    public Map<Long, TagGroupIdentity> retrieveTagGroupIdentities(@Nonnull Set<Long> tagGroupIds) {

        final ImmutableMap.Builder<Long, TagGroupIdentity> tagGroupIdentities = ImmutableMap.builder();

        // First, pull identities from teh identity cache
        final Set<Long> unseenIdentityIds = new HashSet<>();
        tagGroupIds.forEach(tagGroupId -> {
            if (tagGroupIdentityCache.containsKey(tagGroupId)) {
                tagGroupIdentities.put(tagGroupId, tagGroupIdentityCache.get(tagGroupId));
            } else {
                unseenIdentityIds.add(tagGroupId);
            }
        });

        if (!unseenIdentityIds.isEmpty()) {
            streamTagGroupIdentities(unseenIdentityIds).forEach(tagGroupIdentity -> {

                tagGroupIdentityCache.put(tagGroupIdentity.tagGroupId(), tagGroupIdentity);
                tagGroupIdentities.put(tagGroupIdentity.tagGroupId(), tagGroupIdentity);
            });
        }

        return tagGroupIdentities.build();
    }

    private Stream<TagGroupIdentity> streamTagGroupIdentities(@Nonnull Set<Long> tagGroupIds) {

        final Condition groupIdCondition = tagGroupIds.isEmpty()
                ? DSL.trueCondition()
                : CostTagGrouping.COST_TAG_GROUPING.TAG_GROUP_ID.in(tagGroupIds);

        final SetMultimap<Long, Long> tagGroupMap =  dslContext.selectFrom(CostTagGrouping.COST_TAG_GROUPING)
                .where(groupIdCondition)
                .fetch()
                .stream()
                .collect(ImmutableSetMultimap.toImmutableSetMultimap(
                        CostTagGroupingRecord::getTagGroupId,
                        CostTagGroupingRecord::getTagId));

        return tagGroupMap.asMap().entrySet()
                .stream()
                .map(tagGroupEntry -> TagGroupIdentity.of(tagGroupEntry.getKey(), tagGroupEntry.getValue()));
    }

    /**
     * Insert provided tag groups into cost_tag_grouping table.
     *
     * @param tagGroupIdentities tag groups to be inserted into the cost_tag_grouping table.
     * @throws com.vmturbo.sql.utils.DbException on encountering DataAccessException during query execution.
     */
    public void insertCostTagGroups(final Collection<TagGroupIdentity> tagGroupIdentities) throws DbException {

        try {

            logger.debug("Persisting {} tag groups", tagGroupIdentities::size);

            final List<TagGroupIdentity> unseenIdentities = tagGroupIdentities.stream()
                    .filter(tagGroupIdentity -> !tagGroupIdentityCache.containsKey(tagGroupIdentity.tagGroupId()))
                    .collect(ImmutableList.toImmutableList());

            if (!unseenIdentities.isEmpty()) {

                final List<CostTagGroupingRecord> unseenRecords =
                        unseenIdentities.stream().flatMap(tagGroupIdentity -> tagGroupIdentity.tagIds().stream().map(tagId -> {
                            final CostTagGroupingRecord record = new CostTagGroupingRecord();
                            record.setTagGroupId(tagGroupIdentity.tagGroupId());
                            record.setTagId(tagId);
                            return record;
                        })).collect(ImmutableList.toImmutableList());

                dslContext.batch(unseenRecords.stream()
                        .map(unseenRecord -> dslContext.insertInto(CostTagGrouping.COST_TAG_GROUPING).set(unseenRecord).onDuplicateKeyIgnore())
                        .toArray(Query[]::new)).execute();
            }

            unseenIdentities.forEach(tagGroupIdentity -> tagGroupIdentityCache.put(tagGroupIdentity.tagGroupId(), tagGroupIdentity));
        } catch (Exception e) {
            throw new DbException("Failed to insert tag groups into the DB", e);
        }
    }

    @Override
    public Set<Diagnosable> getDiagnosables(boolean collectHistoricalStats) {
        HashSet<Diagnosable> storesToSave = new HashSet<>();
        storesToSave.add(costTagGroupingDiagsHelper);
        return storesToSave;
    }

    @Override
    public void setExportCloudCostDiags(boolean exportCloudCostDiags) {
        this.exportCloudCostDiags = exportCloudCostDiags;
    }

    @Override
    public boolean getExportCloudCostDiags() {
        return this.exportCloudCostDiags;
    }

    /**
     * Helper class for dumping cost tag grouping db records to exported topology.
     */
    private final class CostTagGroupingDiagsHelper implements
            TableDiagsRestorable<Object, CostTagGroupingRecord> {
        private static final String costTagGroupingDumpFile = "costTagGrouping_dump";

        private final DSLContext dsl;

        CostTagGroupingDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext getDSLContext() {
            return dsl;
        }

        @Override
        public TableImpl<CostTagGroupingRecord> getTable() {
            return Tables.COST_TAG_GROUPING;
        }

        @Nonnull
        @Override
        public String getFileName() {
            return costTagGroupingDumpFile;
        }

        @Nonnull
        @Override
        public void collectDiags(@Nonnull final DiagnosticsAppender appender) {
            if (exportCloudCostDiags) {
                TableDiagsRestorable.super.collectDiags(appender);
            } else {
                return;
            }
        }
    }
}