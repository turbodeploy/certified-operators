package com.vmturbo.auth.component.widgetset;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import org.jooq.exception.DataAccessException;
import org.jooq.exception.InvalidResultException;
import org.jooq.exception.NoDataFoundException;
import org.jooq.exception.TooManyRowsException;

import com.vmturbo.auth.component.store.db.tables.records.WidgetsetRecord;
import com.vmturbo.common.protobuf.widgets.Widgets.Widgetset;
import com.vmturbo.common.protobuf.widgets.Widgets.WidgetsetInfo;

public interface IWidgetsetStore {
    /**
     * Persist a new {@link Widgetset} using a new unique ID (OID). The new Widgetset will be
     * "owned" by the given user.
     *
     *
     * @param widgetsetInfo the {@link WidgetsetInfo} to persist
     * @param queryUserOid the oid of the user making this request
     * @return the {@link WidgetsetRecord} as persisted, populated with the newly generated unique OID
     * @throws DataAccessException if a widgetset with the given OID exists; an internal error
     * @throws InvalidResultException if the new record is not inserted; an internal error
     */
    WidgetsetRecord createWidgetSet(WidgetsetInfo widgetsetInfo, long queryUserOid);

    /**
     * Return a Iterator over {@link Widgetset} matching the given categories (or any category if
     * no categories specified) and scope type (or any scope type if not specified). Filtered
     * to include only Widgetsets owned by the given user or marked shared_with_all_users.
     *
     * @param categoriesList the list of categories
     * @param scopeType filter the widgetsets returned to the given scopeType, if any
     * @param queryUserOid the oid of the user making this request
     * @param isUserAdmin whether the user requesting the widgets has an admin role
     * @return an Iterator over all {@link WidgetsetRecord}s that match the given filter
     */
    Iterator<WidgetsetRecord> search(@Nullable List<String> categoriesList,
                                     @Nullable String scopeType, long queryUserOid,
                                     boolean isUserAdmin);

    /**
     * Return an Optional containing the {@link WidgetsetRecord} for the corresponding OID if found,
     * or Optional.empty()if there is no Widgetset with the corresponding OID, or if
     * the Widgetset is not visible to all users and owned by a different user.
     *
     * @param oid the ID to request
     * @param queryUserOid the oid of the user making this request
     * @return an Optional of the {@link WidgetsetRecord} by that OID; Optional.empty() if not found
     * or owned by another user and not shared
     */
    Optional<WidgetsetRecord> fetch(long oid, long queryUserOid);

    /**
     * Store a new copy of the {@link WidgetsetInfo}. Remove any previous value for this OID.
     * The Widgetset to be updated must be "owned" by the given userid.
     *
     * @param widgetsetOid the OID of the widget to update
     * @param WidgetsetInfo the new {@link WidgetsetInfo} with which to update the existing widgetset
     * @param queryUserOid the userid of the user making this request
     * @return the new {@link Widgetset} that was stored
     * @throws NoDataFoundException if a widgetset with the given OID owned by the
     * given user is not found
     * @throws TooManyRowsException if more than one row is updated; this should never happen
     */
    WidgetsetRecord update(long widgetsetOid, WidgetsetInfo WidgetsetInfo, long queryUserOid);

    /**
     * Remove a Widgetset given the OID. Return the Widgetset deleted if found or
     * an empty Widgetset if not. The widgetset to be deleted must be owned by the
     * given user OID.
     *
     * @param oid the unique ID of the {@link Widgetset} to delete
     * @param queryUserOid the Userid of the user making this request
     * @return an Optional containing the previous {@link Widgetset} if found, or Optional.empty()
     *            if not found
     * @throws InvalidResultException if the row is not actually deleted - represents an internal
     * error and should never happen
     */
    Optional<WidgetsetRecord> delete(long oid, long queryUserOid) throws InvalidResultException;

    /**
     * Transfer all of the Widgetsets from the previous users to current user by user OID. We do
     * this operation when deleting the previous user. The Widgetsets to be transferred must be
     * "owned" by the previous user.
     *
     * @param fromUserOids collection of the previous user IDs of the {@link Widgetset}
     * @param toUserOid the current user ID of the {@link Widgetset}
     * @return @return an Iterator over all {@link WidgetsetRecord}s which have been transferred
     */
    Iterator<WidgetsetRecord> transferOwnership(Collection<Long> fromUserOids, long toUserOid);
}
