package com.vmturbo.reports.component.data;

import static com.vmturbo.history.schema.abstraction.tables.Entities.ENTITIES;
import static com.vmturbo.history.schema.abstraction.tables.EntityAssns.ENTITY_ASSNS;
import static com.vmturbo.history.schema.abstraction.tables.EntityAssnsMembersEntities.ENTITY_ASSNS_MEMBERS_ENTITIES;
import static com.vmturbo.history.schema.abstraction.tables.EntityAttrs.ENTITY_ATTRS;
import static com.vmturbo.reports.component.data.ReportDataUtils.getRightSizingInfo;
import static org.jooq.impl.DSL.using;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.reports.component.FailedToInsertGroupException;
import com.vmturbo.reports.component.data.ReportDataUtils.MetaGroup;
import com.vmturbo.reports.component.data.ReportDataUtils.Results;
import com.vmturbo.sql.utils.DbException;

/**
 * A tactical Report DB writer to insert data to support Classic reports.
 * This class is not thread safe and must be synchronized by callers to avoid dead lock.
 */
@NotThreadSafe
public class ReportDBDataWriter {
    private static final String SELECT_LAST_INSERT_ID = "SELECT LAST_INSERT_ID()";
    private static final String ALL_GROUP_MEMBERS = "AllGroupMembers";
    private static final String S_E_TYPE_NAME = "sETypeName";
    private static final String PERSISTING_BATCH_OF_SIZE = "Persisting batch of size: {}";
    private static final int SIZE = 10000;
    private static final String GROUP = "Group";
    @VisibleForTesting
    public static final String STATIC_META_GROUP = "StaticMetaGroup";
    @VisibleForTesting
    public static final String RIGHTSIZING_INFO = "RightsizingInfo";
    //TODO external chunk size when needed.
    private static final int CHUNK_SIZE = 10000;
    // This is a group id represent all VMs. It's used internally and doesn't expect to changes.
    @VisibleForTesting
    public static final String VMS = "VMs";

    // This is a group id represent all PMs. It's used internally and doesn't expect to changes.
    @VisibleForTesting
    public static final String PMS = "PMs";
    private final Logger logger = LogManager.getLogger(getClass());
    private final DSLContext dsl;
    public ReportDBDataWriter(final DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Insert groups to entities table.
     * @param groups groups to be inserted to entities table
     * @param metaGroup Meta Group see {@link MetaGroup} for details
     * @return primary key for the special "VMs" group. And group -> primary key map.
     * @throws DbException if exception is thrown
     */
    public Results insertGroups(@Nonnull final List<Group> groups, @Nonnull final MetaGroup metaGroup) throws DbException {
        logger.info("Populating groups to entities tables. Groups: " + groups);
        // group -> group primary key in entities table
        final Map<Group, Long> groupToPK = Maps.newHashMap();
        try {
            cleanGroup(metaGroup);
            // Special group representing all VMs/PMs/STs
            final Long defaultGroupId =
                insertGroups("", metaGroup.getGroupName(), STATIC_META_GROUP, metaGroup.getGroupPrefix());
            groups.forEach(group -> {
                groupToPK.put(group, insertGroups(group.getCluster().getName(),
                    group.getCluster().getDisplayName(), GROUP, metaGroup.getGroupPrefix()));
                logger.info("Inserting group to entities table, Group name: " + group.getCluster()
                    .getDisplayName());
            });
            return new Results(defaultGroupId, groupToPK);
        } catch (DataAccessException e) {
            throw new DbException("Error inserting groups." + e);
        }
    }

    /**
     * Clean group by Group prefix, e.g. Group-VMsByCluster.
     * @param group MetaGroup which has the group prefix {@link MetaGroup}.
     */
    public void cleanGroup(@Nonnull final MetaGroup group) {
        dsl.transaction(transaction -> {
            final DSLContext transactionContext = using(transaction);
            transactionContext.deleteFrom(ENTITIES).where(ENTITIES.NAME.like(group.getGroupPrefix()+ "%")).execute();
        });
        logger.info("Cleaned up entities tables with name like: " + group.getGroupPrefix() + "%");
    }

    /**
     * Clean up all the rows in entity_assns table where "name" column is "AllGroupMembers"
     * and in 'the groupEntityIds'.
     * @param groupEntityIds group entity ids
     */
    public void cleanUpEntity_Assns(final List<Long> groupEntityIds) throws DbException {
        try {
            dsl.transaction(transaction -> {
                final DSLContext transactionContext = using(transaction);
                transactionContext
                    .deleteFrom(ENTITY_ASSNS)
                    .where(ENTITY_ASSNS.NAME.eq(ALL_GROUP_MEMBERS).and(ENTITY_ASSNS.ENTITY_ENTITY_ID.in(groupEntityIds)))
                    .execute();
            });

        } catch (DataAccessException e) {
            throw new DbException("Error cleaning up entity_assns table." + e);
        }
    }

    /**
     * Insert groups' primary key to entity_assns table. So we know they are "AllGroupMembers".
     * Note: this is child table of entities, when group are remove from entities table,
     * these groups will be "DELETE CASCADE".
     * @param results default group's (e.g. VMs) PK and all cluster groups' PKs.
     */
    public Results insertEntityAssns(final Results results) throws DbException {
        // group -> group primary key in entities table
        final Map<Group, Long> newGroupToPK = Maps.newHashMap();

        final long newDefaultGroupPK  = insertEntityAssnsInternal(results.getDefaultGroupPK());
        results.getGroupToPK().entrySet().forEach(entry ->
            newGroupToPK.put(entry.getKey(), insertEntityAssnsInternal(entry.getValue()))

        );
        return new Results(newDefaultGroupPK, newGroupToPK);
    }


    /**
     * Insert entities (group members) to entity_assns table, so they can be associated to group.
     * @param memberIds member ids
     */
    public void insertEntityAssnsBatch(final List<Long> memberIds) throws DbException {
        try {
            Lists.partition(memberIds, SIZE).forEach(chunk -> {
                dsl.transaction(transaction -> {
                    final DSLContext transactionContext = using(transaction);
                    final BatchBindStep batch = transactionContext.batch(
                        transactionContext.insertInto(ENTITY_ASSNS)
                            .set(ENTITY_ASSNS.NAME, "")
                            .set(ENTITY_ASSNS.ENTITY_ENTITY_ID, 0L)
                    );
                    chunk.forEach(id -> batch.bind(ALL_GROUP_MEMBERS, id));

                    if (batch.size() > 0) {
                        logger.info("Persisting batch of size: {}", batch.size());
                        batch.execute();
                    }
                });
            });
        } catch (DataAccessException e) {
            throw new DbException("Error inserting group members to entity_assns table." + e);
        }
    }

    /**
     * Insert groups' primary keys to entity_attrs table with name equals to "sETypeName".
     * They are cascade deleted with entities table, see entity_attrs table info for details.
     * @param groupIds      group ids
     * @param entityType    entity type
     */
    public void insertEntityAttrs(@Nonnull final List<Long> groupIds,
                                  @Nonnull final String entityType) throws DbException {
        try {
            Lists.partition(groupIds, CHUNK_SIZE).forEach(chunk -> {
                dsl.transaction(transaction -> {
                    final DSLContext transactionContext = using(transaction);
                    // Initialize the batch.
                    final BatchBindStep batch = transactionContext.batch(
                        //have to provide dummy values for jooq
                        transactionContext.insertInto(ENTITY_ATTRS)
                            .set(ENTITY_ATTRS.NAME, "")
                            .set(ENTITY_ATTRS.VALUE, "")
                            .set(ENTITY_ATTRS.ENTITY_ENTITY_ID, 0L)
                    );

                    chunk.forEach(id -> batch.bind(S_E_TYPE_NAME, entityType, id));
                    if (batch.size() > 0) {
                        logger.info(PERSISTING_BATCH_OF_SIZE, batch.size());
                        batch.execute();
                    }
                });
            });
        } catch (DataAccessException e) {
            throw new DbException("Error inserting group names to entity_attrs table." + e);
        }
    }


    /**
     * For every group, insert group primary key and it's members's primary key to entity_assns_members_entities table.
     * @param groupMembersMap groupId -> group members
     */
    public void insertEntityAssnsMembersEntities(final Map<Long, Set<Long>> groupMembersMap) throws DbException {
        try {
            groupMembersMap.entrySet().forEach(entry -> {
                Lists.partition(Lists.newArrayList(entry.getValue()), CHUNK_SIZE).forEach(chunk -> {
                    dsl.transaction(transaction -> {
                        final DSLContext transactionContext = using(transaction);
                        // Initialize the batch.
                        final BatchBindStep batch = transactionContext.batch(
                            //have to provide dummy values for jooq
                            transactionContext.insertInto(ENTITY_ASSNS_MEMBERS_ENTITIES)
                                .set(ENTITY_ASSNS_MEMBERS_ENTITIES.ENTITY_ASSN_SRC_ID, 0L)
                                .set(ENTITY_ASSNS_MEMBERS_ENTITIES.ENTITY_DEST_ID, 0L)
                        );

                        chunk.forEach(id -> batch.bind(entry.getKey(), id));
                        if (batch.size() > 0) {
                            batch.execute();
                        }
                    });
                });
            });
        } catch (DataAccessException e) {
            throw new DbException("Error inserting group members to entity_assn_members_entities table." + e);
        }
    }

    /**
     * Insert right size actions to entity_attrs table
     * @param actionsList right size actions
     */
    public void insertRightSizeActions(final List<ActionSpec> actionsList) throws DbException {
        try {
            dsl.transaction(transaction -> {
                final DSLContext transactionContext = using(transaction);
                transactionContext
                    .deleteFrom(ENTITY_ATTRS)
                    .where(ENTITY_ATTRS.NAME.eq(RIGHTSIZING_INFO))
                    .execute();
            });
            Lists.partition(actionsList, CHUNK_SIZE).forEach(chunk -> {
                dsl.transaction(transaction -> {
                    final DSLContext transactionContext = using(transaction);
                    final BatchBindStep batch = transactionContext.batch(
                        transactionContext.insertInto(ENTITY_ATTRS)
                            .set(ENTITY_ATTRS.NAME, "")
                            .set(ENTITY_ATTRS.VALUE, "")
                            .set(ENTITY_ATTRS.ENTITY_ENTITY_ID, 0L)
                    );

                    chunk.forEach(action -> {
                        final ObjectMapper mapper = new ObjectMapper();
                        try {
                            String json = mapper.writeValueAsString(getRightSizingInfo(action));
                            batch.bind(RIGHTSIZING_INFO, json,
                                action.getRecommendation().getInfo().getResize().getTarget().getId());
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    });
                    if (batch.size() > 0) {
                        logger.info(PERSISTING_BATCH_OF_SIZE, batch.size());
                        batch.execute();
                    }
                });
            });
        } catch (DataAccessException e) {
            throw new DbException("Error inserting right size actions to entity_attrs table." + e);
        }

    }

    /**
     * Insert group to entities table, and return auto increment id
     * @param groupName         group name
     * @param groupDisplayName  display name
     * @param creationClass     creation class
     * @param groupNamePrefix   prefix like Group-VMsByCluster for VM
     * @return auto increment id for the group
     * @throws FailedToInsertGroupException
     */
    private long insertGroups(@Nonnull final String groupName,
                              @Nonnull final String groupDisplayName,
                              @Nonnull final String creationClass,
                              @Nonnull final String groupNamePrefix) throws FailedToInsertGroupException {
        try {
            return dsl.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                context.insertInto(ENTITIES).columns(ENTITIES.NAME, ENTITIES.DISPLAY_NAME, ENTITIES.UUID, ENTITIES.CREATION_CLASS)
                    .values(isStaticGroup(creationClass)? groupNamePrefix : groupNamePrefix + "_hostname\\" +
                        groupDisplayName + "\\" + groupName, groupDisplayName, String.valueOf(IdentityGenerator.next()), creationClass)
                    .execute();
                Result<? extends Record> idResult = context.fetch(SELECT_LAST_INSERT_ID);
                return ((ULong) idResult.get(0).getValue(0)).longValue();
            });
        } catch (DataAccessException e) {
            throw new FailedToInsertGroupException(e);
        }
    }

    private long insertEntityAssnsInternal(long id) {
        try {
            return dsl.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                context.insertInto(ENTITY_ASSNS).columns(ENTITY_ASSNS.NAME, ENTITY_ASSNS.ENTITY_ENTITY_ID)
                    .values(ALL_GROUP_MEMBERS, id)
                    .execute();
                Result<? extends Record> idResult = context.fetch(SELECT_LAST_INSERT_ID);
                logger.debug("Inserting entity_assn table, Group id: " + id);
                return ((ULong) idResult.get(0).getValue(0)).longValue();
            });
        } catch (DataAccessException e) {
            throw new FailedToInsertGroupException(e);
        }
    }

    private boolean isStaticGroup(@Nonnull final String creationClass) {
        return STATIC_META_GROUP.equalsIgnoreCase(creationClass);
    }
}