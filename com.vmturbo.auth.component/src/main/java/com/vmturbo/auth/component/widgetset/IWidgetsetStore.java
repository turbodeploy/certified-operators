package com.vmturbo.auth.component.widgetset;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.jooq.exception.InvalidResultException;
import org.jooq.exception.NoDataFoundException;
import org.jooq.exception.TooManyRowsException;

import com.vmturbo.common.protobuf.widgets.Widgets.Widgetset;
import com.vmturbo.common.protobuf.widgets.Widgets.WidgetsetInfo;

public interface IWidgetsetStore {
    /**
     * Persist a new {@link Widgetset} using a new unique ID (OID). The new Widgetset will be
     * "owned" buy the given user.
     *
     * @param queryUserOid the OID of the user making this request
     * @param widgetsetInfo the {@link WidgetsetInfo} to persist
     * @return the {@link Widgetset} as persisted, populated with the newly generated unique OID
     * @throws TooManyRowsException if a widgetset with the given OID exists; an internal error
     * @throws InvalidResultException if the new record is not inserted; an internal error
     */
    Widgetset createWidgetSet(long queryUserOid, WidgetsetInfo widgetsetInfo);

    /**
     * Return a Iterator over {@link Widgetset} matching the given categories (or any category if
     * no categories specified) and scope type (or any scope type if not specified). Filtered
     * to include only Widgetsets owned by the given user or marked shared_with_all_users.
     *
     * @param queryUserOid the OID of the user making this request
     * @param categoriesList the list of categories
     * @param scopeType filter the widgetsets returned to the given scopeType, if any
     * @return an Iterator over all {@link Widgetset}s that match the given filter
     */
    Iterator<Widgetset> search(long queryUserOid, List<String> categoriesList, String scopeType);

    /**
     * Return an Optional containing the {@link Widgetset} for the corresponding OID if found,
     * or Optional.empty()if there is no Widgetset with the corresponding OID, or if
     * the Widgetset is not visible to all users and owned by a different user.
     *
     * @param queryUserOid the OID of the user making this request
     * @param oid the ID to request
     * @return the {@link Widgetset} by that OID
     */
    Optional<Widgetset> fetch(long queryUserOid, long oid);

    /**
     * Store a new copy of the {@link Widgetset}. Remove any previous value for this OID.
     * The Widgetset to be updated must be "owned" by the given user OID.
     *
     * @param queryUserOid the OID of the user making this request
     * @param widgetsetOid the OID of the widget to update
     * @param WidgetsetInfo the new {@link WidgetsetInfo} with which to update the existing widgetset
     * @return the new {@link Widgetset} that was stored
     * @throws NoDataFoundException if a widgetset with the given OID owned by the
     * given user is not found
     * @throws TooManyRowsException if more than one row is updated; this should never happen
     */
    Widgetset update(long queryUserOid, long widgetsetOid, WidgetsetInfo WidgetsetInfo);

    /**
     * Remove a Widgetset given the OID. Return the Widgetset deleted if found or
     * an empty Widgetset if not. The widgetset to be deleted must be owned by the
     * given user OID.
     *
     * @param queryUserOid the OID of the user making this request
     * @param oid the unique ID of the {@link Widgetset} to delete
     * @return an Optional containing the previous {@link Widgetset} if found, or Optional.empty()
     *            if not found
     * @throws InvalidResultException if the row is not actually deleted - represents an internal
     * error and should never happen
     */
    Optional<Widgetset> delete(long queryUserOid, long oid) throws NoDataFoundException;

}
