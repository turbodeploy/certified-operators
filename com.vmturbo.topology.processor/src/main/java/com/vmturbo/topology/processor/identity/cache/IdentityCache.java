package com.vmturbo.topology.processor.identity.cache;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.longs.LongSet;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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
       * Add an identity record to the cache.
       *
       * @param identityRecord The record to add to the cache.
       * @param loadRport  A LoadReport that the caller can use to log the loading summary.
       * @return the corresponding descriptor of the previous value associated with
       * the identity record, null if there was no mapping.
       */
      @Nullable
      EntityInMemoryProxyDescriptor addIdentityRecord(IdentityRecord identityRecord, LoadReport loadRport);

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

    /**
     * A class that generates an identity cache load report highlighting any potential issues with
     * the loaded identity records.
     */
     class LoadReport {
        private List<IdentityRecord> recsMissingEntityMetadata = new ArrayList<>();
        private List<IdentityRecord> recsMissingIdProps = new ArrayList<>();
        private List<IdentityRecord> recsMissingProbeMetadata = new ArrayList<>();

        void missingEntityMetadata(IdentityRecord identityRecord) {
           recsMissingEntityMetadata.add(identityRecord);
        }

        void missingIdProps(IdentityRecord identityRecord) {
           recsMissingIdProps.add(identityRecord);
        }

        void missingProbeMetadata(IdentityRecord identityRecord) {
           recsMissingProbeMetadata.add(identityRecord);
        }

       /**
        * Generates the load report.
        *
        * @return the report string.
        */
        public String generate() {
           StringWriter sw = new StringWriter();
           PrintWriter pw = new PrintWriter(sw);
           pw.println("IdentityCache Load Summary");
           pw.println(String.format("1. Identity records missing probe metadata: %d", recsMissingProbeMetadata.size()));
           if (recsMissingProbeMetadata.size() > 0) {
              pw.println("Probe metadata not found for the following probe and entity types:");
              printIdentityRecordsByProbe(pw, recsMissingProbeMetadata);
              pw.println();
           }
           pw.println("2. Identity records missing entity metadata: " + recsMissingEntityMetadata.size());
           if (recsMissingEntityMetadata.size() > 0) {
              pw.println("This can happen if the entity type is missing from the entityMetadata part of the probe definition.");
              pw.println("A new OID will be assigned to each entity of these probes and types after each topology-processor restart:");
              printIdentityRecordsByProbe(pw, recsMissingEntityMetadata);
              pw.println();
           }
           pw.println(String.format("3. Identity records missing id props: %d", recsMissingIdProps.size()));
           if (recsMissingIdProps.size() > 0) {
              pw.println("A new OID will be assigned to each entity of these probes and types after each discovery:");
              printIdentityRecordsByProbe(pw, recsMissingIdProps);
           }
           return sw.toString();
        }

        private void printIdentityRecordsByProbe(PrintWriter pw, List<IdentityRecord> records) {
           Map<Long, List<IdentityRecord>> recordsByProbe = records.stream()
                   .collect(Collectors.groupingBy(IdentityRecord::getProbeId));
           for (Entry<Long, List<IdentityRecord>> entryByProbe : recordsByProbe.entrySet()) {
              pw.println("Probe: " + entryByProbe.getKey());
              Map<EntityType, Long> missingByType = entryByProbe.getValue().stream()
                      .collect(Collectors.groupingBy(IdentityRecord::getEntityType, Collectors.counting()));
              // sort by size, greatest to least
              missingByType = missingByType.entrySet().stream()
                      .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                      .collect(Collectors.toMap(Map.Entry::getKey,
                              Map.Entry::getValue,
                              (e1, e2) -> e1, LinkedHashMap::new));
              int count = 0;
              for (Entry<EntityType, Long> entry : missingByType.entrySet()) {
                 count++;
                 pw.println(" " + count + ". " + entry);
              }
           }
        }
     }

}
