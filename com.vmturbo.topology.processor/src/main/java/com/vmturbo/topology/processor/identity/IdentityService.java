package com.vmturbo.topology.processor.identity;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.topology.processor.identity.services.EntityProxyDescriptor;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;

/**
 * The IdentityService implements the identity service.
 */
@NotThreadSafe
public class IdentityService {

    /**
     * The invalid OID. Only used internally. The code outside should always see a valid number.
     */
    public static final long INVALID_OID = IdentityGenerator.nextDummy();

    /**
     * The underlying store.
     */
    private final IdentityServiceUnderlyingStore store_;

    /**
     * The heuristics matcher.
     */
    private final HeuristicsMatcher heuristicsMatcher_;

    /**
     * Constructs the Identity Service.
     *
     * @param storeArg   The underlying store.
     * @param matcherArg The heuristics matcher.
     */
    public IdentityService(final @Nonnull IdentityServiceUnderlyingStore storeArg,
                           final @Nonnull HeuristicsMatcher matcherArg) {
        store_ = checkNotNull(storeArg);
        heuristicsMatcher_ = checkNotNull(matcherArg);
    }


    /**
     * Uses the same algorithm as
     * {@link IdentityService#getEntityOID(EntityDescriptor, EntityMetadataDescriptor)}, but
     * gets multiple OIDs atomically.
     *
     * @param entries A list of the entries to get OIDs for.
     * @return A list of OIDs assigned to the entries. The ID at index i in this list should be
     *         associated with the entry at index i in the input list.
     * @throws IdentityWrongSetException         In case the properties don't constitute the
     *                                              Identity property set.
     * @throws IdentityServiceOperationException In the case of an error interacting with the
     *                                              underlying store.
     * @throws IdentityUninitializedException If the identity service initialization is incomplete.
     */
    public List<Long> getEntityOIDs(@Nonnull final List<EntryData> entries)
            throws IdentityWrongSetException, IdentityServiceOperationException, IdentityUninitializedException {
        final Map<Long, EntryData> entriesToUpdate = new HashMap<>();

        final List<Long> retList = new ArrayList<>(entries.size());
        for (EntryData data : entries) {
            retList.add(getOidToUse(data, entriesToUpdate));
        }

        store_.upsertEntries(entriesToUpdate);

        return Collections.unmodifiableList(retList);
    }

    /**
     * Utility method to get the OID to use for a particular {@link EntryData}. The caller of this
     * method is responsible for persisting the assigned ID to the
     * {@link IdentityServiceUnderlyingStore} in a separate step.
     *
     * @param entryData The data for the entry to assign the OID to.
     * @param entriesToUpsert If the entryData requires an upsert to the database
     *            (e.g. if it's a newly assigned OID, or if volatile properties changed), this
     *            method will add the entryData and the associated ID to this map.
     * @return The OID to use for the input entryData.
     * @throws IdentityWrongSetException         In case the properties don't constitute the
     *                                              Identity property set.
     * @throws IdentityServiceOperationException In the case of an error interacting with the
     *                                              underlying store.
     * @throws IdentityUninitializedException If the identity service initialization is incomplete.
     */
    private long getOidToUse(@Nonnull final EntryData entryData,
                             @Nonnull final Map<Long, EntryData> entriesToUpsert)
            throws IdentityWrongSetException, IdentityServiceOperationException,
                IdentityUninitializedException {
        final EntityDescriptor descriptor = entryData.getDescriptor();
        final EntityMetadataDescriptor metadataDescriptor = entryData.getMetadata();
        final List<PropertyDescriptor> identifyingProperties =
                descriptor.getIdentifyingProperties(metadataDescriptor);
        final List<PropertyDescriptor> volatileProperties =
                descriptor.getVolatileProperties(metadataDescriptor);
        // First, see if we have the match by the identifying properties
        long oid = store_.lookupByIdentifyingSet(metadataDescriptor, identifyingProperties);
        if (oid != INVALID_OID) {
            if (!volatileProperties.isEmpty()) {
                entriesToUpsert.put(oid, entryData);
            }
            return oid;
        }
        // We do not have the match. We might have to perform the search based on non-volatile
        // properties.
        // The volatile properties are the subset of the identifying properties, and
        // the result of descriptor.getVolatileProperties(dtoNow) is fully contained in the
        // result of the descriptor.getIdentifyingProperties(dtoNow).
        // Search for all identifying properties, including volatile ones.
        // We might have a match there.
        Collection<PropertyDescriptor> heuristicsNow =
                descriptor.getHeuristicProperties(metadataDescriptor);
        // We only need non-volatile properties for the query.
        // The reason behind it is the fact that when we get to this point, one or more volatile
        // properties have changed.
        // We will perform the heuristic match, and we need all the Entities that could be a
        // potential hit.
        Collection<PropertyDescriptor> queryIDProps = new ArrayList<>(identifyingProperties);
        queryIDProps.removeAll(volatileProperties);
        if (heuristicsNow != null && heuristicsNow.size() > 0) {
            for (EntityProxyDescriptor match : store_.query(metadataDescriptor, queryIDProps)) {
                // We have volatile properties. Perform heuristics
                Iterable<PropertyDescriptor> heuristicsLast = match.getHeuristicProperties();
                // See if we need to merge the two
                // The VMTHeuristicsMatcher will have to perform the query, locate the existing
                // Entity, and see if match is found.
                // The match is found, return OID.
                if (heuristicsMatcher_.locateMatch(heuristicsLast, heuristicsNow, descriptor, metadataDescriptor)) {
                    entriesToUpsert.put(oid, entryData);
                    return match.getOID();
                }
            }
        }
        // Exhausted all possibilities. No match. Generate a new one.
        oid = IdentityGenerator.next();
        entriesToUpsert.put(oid, entryData);
        return oid;
    }

    /**
     * Obtains or creates Entity. Associates the OID with it as needed. First, it will attempt to
     * match identifying properties. If there is a match, and there will be only 1, return the OID.
     * If not, check whether the identifying property set contains volatile property(ies). If not,
     * we have a new Entity, generate new OID, and add the new Entity to the underlying store. If
     * the volatile property(ies) present, perform the heuristics match. In case the heuristics
     * match returns existing Entity, return that Entity's OID, and update its properties in the
     * underlying store, to reflect the new ones (from DTO). If not, we have a new Entity. Generate
     * the new OID and add the Entity to the underlying store. The different entity types might
     * potentially have the same set of properties. This is handled by the entity subtype being a
     * volatile property. The responsibility for supplying it lies with the probe. This is because
     * the entity may potentially change its type. For example, the VM migrates from VC to Amazon.
     *
     * @param descriptor         The entity descriptor
     * @param metadataDescriptor The entity metadata descriptor.
     * @return The OID.
     * @throws IdentityWrongSetException         In case the properties don't constitute the
     *                                              Identity property set.
     * @throws IdentityServiceOperationException In the case of an error interacting with the
     *                                              underlying store.
     * @throws IdentityUninitializedException If the identity service initialization is incomplete.
     */
    public long getEntityOID(EntityDescriptor descriptor, EntityMetadataDescriptor metadataDescriptor)
            throws IdentityWrongSetException, IdentityServiceOperationException, IdentityUninitializedException {
        return getEntityOIDs(Collections.singletonList(
            new EntryData(descriptor, metadataDescriptor, null))).get(0);
    }

    /**
     * Removes the Entity for the supplied OID. If the entity does not exist, this method does not
     * fail.
     *
     * @param oid The OID.
     * @return {@code true} iff the entity existed and has been removed.
     * @throws IdentityServiceOperationException In the case of an error interacting with the
     *                                              underlying store_.
     * @throws IdentityUninitializedException If the identity service initialization is incomplete.
     */
    public boolean removeEntity(long oid)
            throws IdentityServiceOperationException, IdentityUninitializedException {
        return store_.removeEntry(oid);
    }

    /**
     * Checks whether entity with such OID is already present.
     *
     * @param oid The OID.
     * @return {@code true} iff Entity with such OID is present.
     * @throws IdentityServiceOperationException In case of an issue querying,
     * @throws IdentityUninitializedException If the identity service initialization is incomplete.
     */
    public boolean containsOID(long oid)
            throws IdentityServiceOperationException, IdentityUninitializedException {
        return store_.containsOID(oid);
    }

    /**
     * Checks whether entity with such set of identifying properties is already present.
     *
     * @param metadataDescriptor The metadata descriptor.
     * @param properties         The set of identifying properties.
     * @return {@code true} iff Entity with such set of identifying properties is present.
     * @throws IdentityServiceOperationException In case of an issue querying,
     * @throws IdentityUninitializedException If the identity service initialization is incomplete.
     */
    public boolean containsWithIdentifyingProperties(
                @Nonnull EntityMetadataDescriptor metadataDescriptor,
                @Nonnull List<PropertyDescriptor> properties)
            throws IdentityServiceOperationException, IdentityUninitializedException {
        return store_.containsWithIdentifyingProperties(metadataDescriptor, properties);
    }

    /**
     * Write out the contents of the {@link IdentityService} to the provided writer.
     *
     * @param writer The writer to write to.
     */
    public void backup(@Nonnull final Writer writer) {
        store_.backup(writer);
    }

    /**
     * Restore the contents of a backed-up {@link IdentityService} from the provided reader.
     *
     * @param reader The reader to read from.
     */
    public void restore(@Nonnull final Reader reader) {
        store_.restore(reader);
    }
}
