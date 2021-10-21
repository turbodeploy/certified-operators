package com.vmturbo.topology.processor.identity.cache;

import java.io.Writer;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.longs.LongSet;

import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.storage.EntityInMemoryProxyDescriptor;
import com.vmturbo.topology.processor.identity.storage.IdentityRecord;

/**
 * Identity cache Interface. The purpose of the identity cache is to handle data regarding entity oids.
 * This includes a mapping  oid to entity descriptor, a mapping from identifying properties to oid and
 * a mapping from non volatile properties to oid. The reason we have three implementations of the cache
 * is to support older set of diags that do not contain {@link IdentityRecord}, but only contains
 * {@link EntityInMemoryProxyDescriptor}. Once we no longer need to support older set of diags we
 * can remove the {@link DescriptorsBasedCache}.
 */
  public interface IdentityCache {

     /**
      * Add an identity record to the cache.
      *
      * @param identityRecord The record to add to the cache.
      * @return the corresponding descriptor of the previous value associated with
      * the identity record, null if there was no mapping.
      */
     @Nullable
     EntityInMemoryProxyDescriptor addIdentityRecord(IdentityRecord identityRecord);

     /**
      * Checks if the oid is contained in the cache.
      *
      * @param oid of the entity.
      * @return true if the entity is in the cache.
      */
     boolean containsKey(long oid);

     /**
      * Remove several entities from the cache by giving the corresponding oid.
      *
      * @param oidsToRemove of the entity.
      * @return the number of entities that got removed.
      */
     int remove(Set<Long> oidsToRemove);

     /**
      * Get the {@link EntityInMemoryProxyDescriptor} by the oid of the entity.
      *
      * @param oid of the entity.
      * @return the corresponding {@link EntityInMemoryProxyDescriptor} .
      */
     EntityInMemoryProxyDescriptor get(Long oid);

     /**
      * Writes the content of the cache into json format.
      *
      * @param writer on which the json is written.
      */
     void toJson(@Nonnull Writer writer);

     /**
      * Clear the cache.
      */
     void clear();

     /**
      * Add a descriptor to the cache.
      *
      * @param descriptor to add to the cache.
      * @return the corresponding {@link EntityInMemoryProxyDescriptor} .
      */
     EntityInMemoryProxyDescriptor addDescriptor(EntityInMemoryProxyDescriptor descriptor);

     /**
      * Given a list of identifying properties, return all the oids of the entities that match
      * those identifying properties.
      *
      * @param nonVolatileProperties to match.
      * @param volatileProperties to match.
      * @return the corresponding oid.
      */
     long getOidByIdentifyingProperties(@Nonnull List<PropertyDescriptor> nonVolatileProperties,
             @Nonnull List<PropertyDescriptor> volatileProperties);

     /**
      * Given a list of non volatile properties, return all the {@link EntityInMemoryProxyDescriptor}
      * of the entities that match those non volatile properties.
      *
      * @param properties to match.
      * @return the corresponding {@link EntityInMemoryProxyDescriptor}.
      */
     List<EntityInMemoryProxyDescriptor> getDtosByNonVolatileProperties(
         @Nonnull List<PropertyDescriptor> properties);

     /**
      * Prints the size of the elements inside the cache. This call can be expensive and
      * should be avoided in production, unless its set to run on specific log levels and
      * intended for debugging only.
      */
     void report();

     /**
      * Get all the oids that are currently present in the cache.
      * @return a {@link LongSet} with the oids
      */
     Set<Long> getOids();

}
