package com.vmturbo.auth.component.widgetset;

import static com.vmturbo.auth.component.store.db.Tables.WIDGETSET;

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
import org.jooq.SelectQuery;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.InvalidResultException;
import org.jooq.exception.NoDataFoundException;
import org.jooq.exception.TooManyRowsException;
import org.jooq.impl.DSL;

import com.vmturbo.auth.component.store.db.tables.pojos.Widgetset;
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
     * Persist a new {@link Widgets.Widgetset} using a new unique ID (OID). The new Widgetset will be
     * "owned" by the given user.
     * Try first to fetch the given OID to ensure it doesn't already exist. As the caller is likely
     * using {@link IdentityGenerator} this is very unlikely. When not found convert to a DB record,
     * add the new OID, and store it, checking the number of records written.
     *
     * @param queryUserOid the OID of the user making this request
     * @param newWidgetsetInfo the {@link Widgets.WidgetsetInfo} to persist
     * @return the {@link Widgets.Widgetset} as persisted, populated with the new OID
     * @throws DataAccessException if a widgetset with the given OID exists
     * @throws NoDataFoundException if the new record is not inserted
     */
    @Override
    public Widgets.Widgetset createWidgetSet(long queryUserOid,
                                             @Nonnull Widgets.WidgetsetInfo newWidgetsetInfo)
            throws DataAccessException, NoDataFoundException {
        // generate a new ID for the new widgetset
        final long nextOid = IdentityGenerator.next();
        // insert that into a new Widgetset structure to be inserted and returned
        final Widgets.Widgetset updatedWidgetset = Widgets.Widgetset.newBuilder()
                .setOid(nextOid)
                .setOwnerOid(queryUserOid)
                .setInfo(newWidgetsetInfo)
                .build();
        dsl.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            // convert to a DB Widgetset table row
            Widgetset dbWidgetset = toDbWidgetset(updatedWidgetset);
            // force the owner ID to the userid of the requestor
            dbWidgetset.setOwnerOid(queryUserOid);
            // the Widgetset OID is marked as unique in the SQL table definition
            int nStored = transactionDsl.newRecord(WIDGETSET, dbWidgetset).store();
            if (nStored != 1) {
                throw new NoDataFoundException("Error storing new widget:  " + nStored +
                        " instances for " + nextOid + " for user " + queryUserOid);
            }
        });
        return updatedWidgetset;
    }

    /**
     * Return a Iterator over {@link Widgets.Widgetset} matching the given categories (or any category if
     * no categories specified) and scope type (or any scope type if not specified). Filtered
     * to include only Widgetsets owned by the given user or marked shared_with_all_users.
     * The query.stream() should implement a lazy iterator, fetching records when requested
     * by the processing in the caller.
     *
     * @param queryUserOid the OID of the user making this request
     * @param categoriesList the list of categories
     * @param scopeType filter the widgetsets returned to the given scopeType, if any
     * @return an Iterator over all {@link Widgets.Widgetset}s that match the given filter
     */

    @Override
    public Iterator<Widgets.Widgetset> search(long queryUserOid,
                                              @Nullable List<String> categoriesList,
                                              @Nullable String scopeType) {

        SelectQuery<WidgetsetRecord> query = dsl.selectQuery(WIDGETSET);
        query.addConditions(WIDGETSET.SHARED_WITH_ALL_USERS.ne(DB_FALSE)
                .or(WIDGETSET.OWNER_OID.eq(queryUserOid)));
        if (CollectionUtils.isNotEmpty(categoriesList)) {
            query.addConditions(WIDGETSET.CATEGORY.in(categoriesList));
        }
        if (StringUtils.isNotEmpty(scopeType)){
            query.addConditions(WIDGETSET.SCOPE_TYPE.eq(scopeType));
        }
        return query.stream().map(this::fromDbWidgetset).iterator();
    }

    /**
     * Return the {@link Widgets.Widgetset} for the corresponding OID. Throw {@link NotFoundException}
     * if there is no Widgetset with the corresponding OID or if
     * the Widgetset is not visible to all users and owned by a different user.
     *
     * @param queryUserOid the OID of the user making this request
     * @param oid the ID to request
     * @return the {@link Widgets.Widgetset} by that OID
     * @throws NoDataFoundException if a Widgetset by that OID is not found
     */

    @Override
    public Optional<Widgets.Widgetset> fetch(long queryUserOid, long oid) {
        WidgetsetRecord result = dsl.selectFrom(WIDGETSET).where(WIDGETSET.OID.eq(oid)
                .and(WIDGETSET.OWNER_OID.eq(queryUserOid)
                        .or(WIDGETSET.SHARED_WITH_ALL_USERS.ne(DB_FALSE))))
                .fetchOne();
        if (result == null) {
            logger.debug("Cannot find widgetset with ID " + oid +
                    " for user " + queryUserOid);
            return Optional.empty();
        }
        return Optional.of(fromDbWidgetset(result));
    }

    /**
     * Store a new copy of the {@link Widgets.Widgetset}. Remove any previous value for this OID.
     * The Widgetset to be updated must be "owned" by the given user OID.
     * Use the "update" verb and check the count of records updated. If the record is not updated,
     * this means that either the record doesn't exist or it is not owned by the given user, and
     * a NoDataFound exception will be thrown. If more than one record is updated this represents
     * multiple records for the same Widgetset, and a TooManyRowsException will be thrown.
     *
     * @param queryUserOid the OID of the user making this request
     * @param newWidgetsetInfo the new {@link Widgets.Widgetset} to store
     * @return the new {@link Widgets.Widgetset} that was stored
     * @throws NoDataFoundException if a widgetset with the given OID owned by the
     * given user is not found
     * @throws TooManyRowsException if more than one row is updated; this should never happen
     */

    @Override
    public Widgets.Widgetset update(long queryUserOid, long oid,
                                    @Nonnull Widgets.WidgetsetInfo newWidgetsetInfo)
            throws NoDataFoundException {

        Widgets.Widgetset.Builder updatedWidgetset = Widgets.Widgetset.newBuilder();
        dsl.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            Widgetset dbWidgetset = toDbWidgetset(Widgets.Widgetset.newBuilder()
                    .setOid(oid)
                    .setOwnerOid(queryUserOid)
                    .setInfo(newWidgetsetInfo));
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
            WidgetsetRecord updatedDbRecord = transactionDsl.selectFrom(WIDGETSET)
                    .where(WIDGETSET.OID.eq(oid))
                    .fetchOne();
            updatedWidgetset.mergeFrom(fromDbWidgetset(updatedDbRecord));
        });
        return updatedWidgetset.build();
    }

    /**
     * Remove a Widgetset given the OID. Return the Widgetset deleted if found or
     * an empty Widgetset if not. The widgetset to be deleted must be owned by the
     * given user OID.
     * In a transaction, fetch the record to be deleted. This value will be returned.
     * If not found, or not owned by the requesting user, throw NoDataFoundException.
     *
     * If the 'delete()' count is not one, then there was an error deleting the given
     * widgetset; throw
     *
     *
     * @param queryUserOid the OID of the user making this request
     * @param oid the unique ID of the {@link Widgets.Widgetset} to delete
     * @return the previous {@link Widgets.Widgetset} if found, or a new empty Widgetset if
     *            not found
     * @throws NoDataFoundException if a widgetset with the given OID owned by the
     * given user is not found
     * @throws InvalidResultException if the number of rows deleted is not exactly '1'
     */
    @Override
    public Optional<Widgets.Widgetset> delete(long queryUserOid, long oid) throws
            InvalidResultException {
        Widgets.Widgetset.Builder deletedWidgetset = Widgets.Widgetset.newBuilder();
        dsl.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            WidgetsetRecord dbRecordToDelete = transactionDsl.selectFrom(WIDGETSET)
                    .where(WIDGETSET.OID.eq(oid)
                            .and(WIDGETSET.OWNER_OID.eq(queryUserOid)))
                    .fetchOne();
            if (dbRecordToDelete == null) {
                logger.warn("Could not delete widgetset with OID " + oid);
                return;
            }
            deletedWidgetset.mergeFrom(fromDbWidgetset(dbRecordToDelete));

            int deleted = transactionDsl.deleteFrom(WIDGETSET)
                    .where(WIDGETSET.OID.eq(oid))
                    .execute();
            if (deleted != 1) {
                throw new InvalidResultException("Error deleting widgetset " + oid +
                        "; wrong number of rows deleted: " + deleted);
            }
        });

        if (!deletedWidgetset.hasOid()) {
            // widgetset to delete was not found
            return Optional.empty();
        }
        return Optional.of(deletedWidgetset.build());
    }


    private Widgetset toDbWidgetset(Widgets.WidgetsetOrBuilder protoWidgetset) {
        Widgetset dbWidgetset = new Widgetset();
        if (protoWidgetset.hasOid()) {
            dbWidgetset.setOid(protoWidgetset.getOid());
        }
        if (protoWidgetset.hasOwnerOid()) {
            dbWidgetset.setOwnerOid(protoWidgetset.getOwnerOid());
        }
        if (protoWidgetset.hasInfo()) {
            Widgets.WidgetsetInfo protoInfo = protoWidgetset.getInfo();
            if (protoInfo.hasCategory()) {
                dbWidgetset.setCategory(protoInfo.getCategory());
            }
            if (protoInfo.hasScope()) {
                dbWidgetset.setScope(protoInfo.getScope());
            }
            if (protoInfo.hasScopeType()) {
                dbWidgetset.setScopeType(protoInfo.getScopeType());
            }
            if (protoInfo.hasSharedWithAllUsers()) {
                dbWidgetset.setSharedWithAllUsers((byte) (protoInfo.getSharedWithAllUsers() ? 1 : 0));
            }
            if (protoInfo.hasWidgets()) {
                dbWidgetset.setWidgets(protoInfo.getWidgets());
            }
        }
        return dbWidgetset;
    }

    private Widgets.Widgetset fromDbWidgetset(WidgetsetRecord dbWidgetset) {
        Widgets.Widgetset.Builder protoWidgetset = Widgets.Widgetset.newBuilder();
        if (dbWidgetset.getOid() != null) {
            protoWidgetset.setOid(dbWidgetset.getOid());
        }
        if (dbWidgetset.getOwnerOid() != null) {
            protoWidgetset.setOwnerOid(dbWidgetset.getOwnerOid());
        }
        Widgets.WidgetsetInfo.Builder protoWidgetsetInfo = Widgets.WidgetsetInfo.newBuilder();
        if (dbWidgetset.getCategory() != null) {
            protoWidgetsetInfo.setCategory(dbWidgetset.getCategory());
        }
        if (dbWidgetset.getScope() != null) {
            protoWidgetsetInfo.setScope(dbWidgetset.getScope());
        }
        if (dbWidgetset.getScopeType() != null) {
            protoWidgetsetInfo.setScopeType(dbWidgetset.getScopeType());
        }
        if (dbWidgetset.getSharedWithAllUsers() != null) {
            protoWidgetsetInfo.setSharedWithAllUsers(dbWidgetset.getSharedWithAllUsers() != 0);
        }
        if (dbWidgetset.getWidgets() != null) {
            protoWidgetsetInfo.setWidgets(dbWidgetset.getWidgets());
        }
        return protoWidgetset.setInfo(protoWidgetsetInfo).build();
    }


}
