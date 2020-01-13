package com.vmturbo.auth.component.widgetset;

import static com.vmturbo.auth.component.store.db.Tables.WIDGETSET;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.NotFoundException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SelectQuery;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.InvalidResultException;
import org.jooq.exception.NoDataFoundException;
import org.jooq.exception.TooManyRowsException;
import org.jooq.impl.DSL;

import com.google.common.collect.Lists;

import com.vmturbo.auth.component.store.db.tables.records.WidgetsetRecord;
import com.vmturbo.common.protobuf.widgets.Widgets;
import com.vmturbo.commons.idgen.IdentityGenerator;

/**
 * Storage for Widgetsets using MariaDB.
 **/
public class WidgetsetDbStore implements IWidgetsetStore {

    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    private static final byte DB_FALSE = 0;

    public WidgetsetDbStore(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Persist a new {@link Widgets.WidgetsetInfo} using a new unique ID (OID). The new Widgetset will be
     * "owned" by the given user.
     * Try first to fetch the given OID to ensure it doesn't already exist. As the caller is likely
     * using {@link IdentityGenerator} this is very unlikely. When not found convert to a DB record,
     * add the new OID, and store it, checking the number of records written.
     *
     * @param newWidgetsetInfo the {@link Widgets.WidgetsetInfo} to persist
     * @param queryUserOid the OID of the user making this request
     * @return the {@link Widgets.Widgetset} as persisted, populated with the new OID
     * @throws DataAccessException if a widgetset with the given OID exists
     * @throws NoDataFoundException if the new record is not inserted
     */
    @Override
    public WidgetsetRecord createWidgetSet(@Nonnull Widgets.WidgetsetInfo newWidgetsetInfo, long queryUserOid)
            throws  NoDataFoundException {
        // generate a new ID for the new widgetset
        final long nextOid = IdentityGenerator.next();
        // insert that into a new WidgetsetRecord structure to be inserted and returned
        WidgetsetRecord dbWidgetset = toDbWidgetset(nextOid, newWidgetsetInfo, queryUserOid);
        dsl.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            // convert to a DB Widgetset table row
            // force the owner ID to the userid of the requestor
            dbWidgetset.setOwnerOid(queryUserOid);
            // the Widgetset OID is marked as unique in the SQL table definition
            int nStored = transactionDsl.newRecord(WIDGETSET, dbWidgetset).store();
            if (nStored != 1) {
                throw new NoDataFoundException("Error storing new widget:  " + nStored +
                    " instances for " + nextOid + " for user " + queryUserOid);
            }
        });
        return dbWidgetset;
    }

    /**
     * Return a Iterator over {@link WidgetsetRecord} matching the given categories (or any category if
     * no categories specified) and scope type (or any scope type if not specified). Filtered
     * to include only Widgetsets owned by the given user or marked shared_with_all_users.
     * The query.stream() should implement a lazy iterator, fetching records when requested
     * by the processing in the caller.
     *
     * @param categoriesList the list of categories
     * @param scopeType filter the widgetsets returned to the given scopeType, if any
     * @param queryUserOid the OID of the user making this request
     * @return an Iterator over all {@link WidgetsetRecord}s that match the given filter
     */

    @Override
    public Iterator<WidgetsetRecord> search(@Nullable List<String> categoriesList,
                                            @Nullable String scopeType, long queryUserOid) {

        SelectQuery<WidgetsetRecord> query = dsl.selectQuery(WIDGETSET);
        query.addConditions(WIDGETSET.SHARED_WITH_ALL_USERS.ne(DB_FALSE)
            .or(WIDGETSET.OWNER_OID.eq(queryUserOid)));
        if (CollectionUtils.isNotEmpty(categoriesList)) {
            query.addConditions(WIDGETSET.CATEGORY.in(categoriesList));
        }
        if (StringUtils.isNotEmpty(scopeType)){
            query.addConditions(WIDGETSET.SCOPE_TYPE.eq(scopeType));
        }
        return query.iterator();
    }

    /**
     * Return the {@link WidgetsetRecord} for the corresponding OID. Throw {@link NotFoundException}
     * if there is no Widgetset with the corresponding OID or if
     * the Widgetset is not visible to all users and owned by a different user.
     *
     * @param oid the ID to request
     * @param queryUserOid the OID of the user making this request
     * @return the {@link WidgetsetRecord} by that OID
     * @throws NoDataFoundException if a Widgetset by that OID is not found
     */

    @Override
    public Optional<WidgetsetRecord> fetch(long oid, long queryUserOid) {
        WidgetsetRecord result = dsl.selectFrom(WIDGETSET)
            .where(WIDGETSET.OID.eq(oid)
            .and(WIDGETSET.OWNER_OID.eq(queryUserOid)
            .or(WIDGETSET.SHARED_WITH_ALL_USERS.ne(DB_FALSE))))
            .fetchOne();
        if (result == null) {
            logger.debug("Cannot find widgetset with ID " + oid +
                " for user " + queryUserOid);
        }
        return Optional.ofNullable(result);
    }

    /**
     * Store a new copy of the {@link Widgets.Widgetset}. Remove any previous value for this OID.
     * The Widgetset to be updated must be "owned" by the given user OID.
     * Use the "update" verb and check the count of records updated. If the record is not updated,
     * this means that either the record doesn't exist or it is not owned by the given user, and
     * a NoDataFound exception will be thrown. If more than one record is updated this represents
     * multiple records for the same Widgetset, and a TooManyRowsException will be thrown.
     *
     * @param newWidgetsetInfo the new {@link Widgets.Widgetset} to store
     * @param queryUserOid the OID of the user making this request
     * @return the new {@link Widgets.Widgetset} that was stored
     * @throws NoDataFoundException if a widgetset with the given OID owned by the
     * given user is not found
     * @throws TooManyRowsException if more than one row is updated; this should never happen
     */
    @Override
    public WidgetsetRecord update(long oid, @Nonnull Widgets.WidgetsetInfo newWidgetsetInfo,
                                  long queryUserOid)
            throws NoDataFoundException {

        // The new WidgetsetInfo will replace the prior
        WidgetsetRecord dbWidgetset = toDbWidgetset(oid, newWidgetsetInfo, queryUserOid);
        dsl.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            int count = transactionDsl.executeUpdate(transactionDsl.newRecord(WIDGETSET, dbWidgetset),
                WIDGETSET.OID.eq(oid).and(WIDGETSET.OWNER_OID.eq(queryUserOid)));
            if (count == 0) {
                throw new NoDataFoundException("Cannot update widgetset id " + oid +
                    " for user oid " + queryUserOid);
            }
            if (count > 1) {
                throw new TooManyRowsException("Internal error: " + count +
                    " records updated for oid " + oid);
            }
        });
        return dbWidgetset;
    }

    /**
     * Remove a Widgetset given the OID. Return the Widgetset deleted if found or
     * an empty Widgetset if not. The widgetset to be deleted must be owned by the
     * given user OID.
     *
     * In a transaction, fetch the record to be deleted. An Optional of this value will be returned.
     * If not found, or not owned by the requesting user, Optional.empty() will be returned.
     *
     * If the 'delete()' count is not one, then there was an error deleting the given
     * widgetset; throw InvalidResultException.
     *
     *
     * @param widgetOid the unique ID of the {@link WidgetsetRecord} to delete
     * @param queryUserOid the OID of the user making this request
     * @return an Optional containing the previous {@link WidgetsetRecord} that was deleted,
     * if found, or Optional.empty() if not found or not deletable.
     * @throws InvalidResultException if the number of rows deleted is not exactly '1' - should not
     * happen
     */
    @Override
    public Optional<WidgetsetRecord> delete(long widgetOid, long queryUserOid) throws
            InvalidResultException {
        // create an array here because variables accessed inside the lambda must be
        // effectively final; if the transaction throws an exception, answer[0] will be null
        WidgetsetRecord[] answer = new WidgetsetRecord[1];
        try {
            dsl.transaction(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);
                WidgetsetRecord dbRecordToDelete = transactionDsl.selectFrom(WIDGETSET)
                    .where(WIDGETSET.OID.eq(widgetOid)
                    .and(WIDGETSET.OWNER_OID.eq(queryUserOid)))
                    .fetchOne();
                if (dbRecordToDelete == null) {
                    throw new NoDataFoundException("Could not find widgetset with OID " +
                        widgetOid + " owned by " + queryUserOid + " to delete.");
                }
                answer[0] = dbRecordToDelete;
                int deleted = transactionDsl.deleteFrom(WIDGETSET)
                    .where(WIDGETSET.OID.eq(widgetOid))
                    .execute();
                if (deleted != 1) {
                    throw new InvalidResultException("Error deleting widgetset " + widgetOid +
                        "; wrong number of rows deleted: " + deleted);
                }
            });
        } catch (NoDataFoundException e) {
            // widgetset to delete was not found or owned by another user
            logger.debug(e.getMessage());
        }
        // return the deleted widgetset structure
        return Optional.ofNullable(answer[0]);
    }

    @Override
    public Iterator<WidgetsetRecord> transferOwnership(Collection<Long> fromUserOids, long toUserOid) {
        final List<WidgetsetRecord> records = Lists.newArrayList();
        dsl.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            Result<WidgetsetRecord> dbRecordToTransfer = transactionDsl.selectFrom(WIDGETSET)
                .where(WIDGETSET.OWNER_OID.in(fromUserOids))
                .fetch();
            dbRecordToTransfer.forEach(widgetsetRecord -> {
                widgetsetRecord.setOwnerOid(toUserOid);
            });
            final int[] updatesCount = transactionDsl
                .batchUpdate(dbRecordToTransfer)
                .execute();
            for (int i = 0; i < updatesCount.length; i ++) {
                // succeed
                if (updatesCount[i] == 1) {
                    records.add(dbRecordToTransfer.get(i));
                }
            }
        });
        return records.iterator();
    }


    /**
     * Create a DB Widgetset instance to store in the DB. Populate the OID and OWNER_OID fields
     * from the parameters, and the other columns from the WidgetsetInfo protobuf.
     *
     * @param widgetsetOid the unique ID for this widgetset row
     * @param widgetsetInfoProto the details of the widgetset
     * @param ownerOid the OID for the user who created/owns this widgetset
     * @return a DB Widgetset pojo (JOOQ) for persisting the WidgetsetInfo protobuf given
     */
    private WidgetsetRecord toDbWidgetset(long widgetsetOid,
                                          @Nonnull Widgets.WidgetsetInfo widgetsetInfoProto,
                                          long ownerOid) {
        WidgetsetRecord dbWidgetset = new WidgetsetRecord();
        dbWidgetset.setOid(widgetsetOid);
        dbWidgetset.setOwnerOid(ownerOid);
        if (widgetsetInfoProto.hasDisplayName()) {
            dbWidgetset.setDisplayName(widgetsetInfoProto.getDisplayName());
        }
        if (widgetsetInfoProto.hasCategory()) {
            dbWidgetset.setCategory(widgetsetInfoProto.getCategory());
        }
        if (widgetsetInfoProto.hasScope()) {
            dbWidgetset.setScope(widgetsetInfoProto.getScope());
        }
        if (widgetsetInfoProto.hasScopeType()) {
            dbWidgetset.setScopeType(widgetsetInfoProto.getScopeType());
        }
        if (widgetsetInfoProto.hasSharedWithAllUsers()) {
            dbWidgetset.setSharedWithAllUsers((byte) (widgetsetInfoProto.getSharedWithAllUsers() ? 1 : 0));
        }
        if (widgetsetInfoProto.hasWidgets()) {
            dbWidgetset.setWidgets(widgetsetInfoProto.getWidgets());
        }
        return dbWidgetset;
    }
}
