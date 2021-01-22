package com.vmturbo.topology.processor.identity.storage;

import static com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore.composeKeyFromProperties;

import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.memory.MemoryMeasurer;
import com.vmturbo.common.protobuf.memory.MemoryMeasurer.MemoryMeasurement;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadata;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadataStore;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityProperty;

/**
 * Identity cache class. It contains the IdentityCache interface and the implementations. The
 * purpose of the identity cache is to handle data regarding entity oids. This includes a mapping
 * oid to entity descriptor, a mapping from identifying properties to oid and a mapping from non
 * volatile properties to oid. The reason we have two implementations of the cache is to support
 * older set of diags that do not contain {@link IdentityRecord}, but only contains
 * {@link EntityInMemoryProxyDescriptor}. Once we no longer need to support older set of diags we
 * can remove the {@link DescriptorsBasedCache}.
 */
public class IdentityCaches {

    /**
     * Identity cache interface.
     */
     public interface IdentityCache {

        /**
         * Add an identity record to the cache.
         *
         * @param identityRecord The record to add to the cache.
         * @return the corresponding descriptor of the previous value associated with
         * the identity record, null if there was no mapping.
         */
        EntityInMemoryProxyDescriptor addIdentityRecord(IdentityRecord identityRecord);

        /**
         * Checks if the oid is contained in the cache.
         *
         * @param oid of the entity.
         * @return true if the entity is in the cache.
         */
        boolean containsKey(long oid);

        /**
         * Remove an entity from the cache by giving the corresponding oid.
         *
         * @param oid of the entity.
         * @return the entity that got removed, null if the entity was not in the cache.
         */
        EntityInMemoryProxyDescriptor remove(Long oid);

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
         * Get the number of elements contained in the cache.
         *
         * @return the amount of elements.
         */
        int size();

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
         * @param properties to match.
         * @return the corresponding oids.
         */
        long getOidByIdentifyingProperties(@Nonnull List<PropertyDescriptor> properties);

        /**
         * Given a list of non volatile properties, return all the oids of the entities that match
         * those non volatile properties.
         *
         * @param properties to match.
         * @return the corresponding oids.
         */
        List<EntityInMemoryProxyDescriptor> getDtosByNonVolatileProperties(
            @Nonnull List<PropertyDescriptor> properties);

        /**
         * Prints the size of the elements inside the cache. This call can be expensive and
         * should be avoided in production, unless its set to run on specific log levels and
         * intended for debugging only.
         */
        void report();
    }

    /**
     * Identity that works with {@link IdentityRecord}. This cache is able to distinguish between
     * volatile and non volatile properties among entities and is optimized to lookup entities by
     * non volatile properties.
     */
    public static class IdentityRecordsBasedCache implements IdentityCache {

        private  Map<Long, IdentityRecord> oidToIdentity;

        private final ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata;

        private final Logger logger =
            LogManager.getLogger(IdentityServiceInMemoryUnderlyingStore.class);

        private final Map<String, LongArrayList> nonVolatilePropertiesToOid = new HashMap<>();

        /**
         * Map from the combined identifying properties to OID.
         */
        private final Map<String, Long> identifyingPropertiesToOid = new Object2LongOpenHashMap<>();

        /**
         * Get a IdentityRecordsBasedCache.
         * @param perProbeMetadata map containing probe it to probe metadata
         */
        public IdentityRecordsBasedCache(@Nonnull ConcurrentMap<Long,
            ServiceEntityIdentityMetadataStore> perProbeMetadata) {
            this.perProbeMetadata = perProbeMetadata;
            this.oidToIdentity = Collections.synchronizedMap(new Long2ObjectOpenHashMap<>());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public EntityInMemoryProxyDescriptor addIdentityRecord(IdentityRecord identityRecord) {
            IdentityRecord existingRecord =
                oidToIdentity.put(identityRecord.getDescriptor().getOID(),
                identityRecord);
            identifyingPropertiesToOid.put(identityRecord.getDescriptor().getKey(), identityRecord.getDescriptor().getOID());
            List<PropertyDescriptor> nonVolatileProperties =
                extractNonVolatileProperties(identityRecord);
            nonVolatilePropertiesToOid.computeIfAbsent(composeKeyFromProperties(nonVolatileProperties),
                k -> new LongArrayList()).add(identityRecord.getDescriptor().getOID());
            return existingRecord != null ? existingRecord.getDescriptor() : null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public  boolean containsKey(long oid) {
            return oidToIdentity.containsKey(oid);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public EntityInMemoryProxyDescriptor remove(Long oid) {
            IdentityRecord existingRecord = oidToIdentity.remove(oid);
            if (existingRecord != null) {
                identifyingPropertiesToOid.remove(existingRecord.getDescriptor().getKey());
                List<PropertyDescriptor> nonVolatileProperties =
                    extractNonVolatileProperties(existingRecord);
                String serializedNonVolatileProps = composeKeyFromProperties(nonVolatileProperties);
                LongArrayList matchingOids = nonVolatilePropertiesToOid.get(serializedNonVolatileProps);
                if (matchingOids != null) {
                    LongArrayList filteredOids = new LongArrayList(
                        matchingOids.stream().filter(matchingOid -> !matchingOid.equals(oid)).collect(Collectors.toList()));
                    nonVolatilePropertiesToOid.put(serializedNonVolatileProps, filteredOids);
                }
                return existingRecord.getDescriptor();
            }
            return null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public EntityInMemoryProxyDescriptor get(Long oid) {
            IdentityRecord existingRecord = oidToIdentity.get(oid);
            return existingRecord != null ? existingRecord.getDescriptor() : null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void toJson(@Nonnull final Writer writer) {
            final EntityInMemoryProxyDescriptorConverter converter =
                new EntityInMemoryProxyDescriptorConverter();
            final GsonBuilder builder = new GsonBuilder();
            builder.registerTypeAdapter(EntityInMemoryProxyDescriptor.class,
                (JsonSerializer<EntityInMemoryProxyDescriptor>)(descriptor, type, jsonSerializationContext) -> new JsonPrimitive(converter.to(descriptor)));
            builder.create().toJson(oidToIdentity.values(), writer);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int size() {
            return oidToIdentity.keySet().size();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void clear() {
            oidToIdentity.clear();
            identifyingPropertiesToOid.clear();
            nonVolatilePropertiesToOid.clear();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public EntityInMemoryProxyDescriptor addDescriptor(EntityInMemoryProxyDescriptor descriptor) {
            logger.error("This method is not supported for this type of cache");
            return null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getOidByIdentifyingProperties(@Nonnull List<PropertyDescriptor> properties) {
            return identifyingPropertiesToOid.getOrDefault(composeKeyFromProperties(properties),
                IdentityService.INVALID_OID);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public List<EntityInMemoryProxyDescriptor> getDtosByNonVolatileProperties(
            @Nonnull final List<PropertyDescriptor> properties) {
            List<EntityInMemoryProxyDescriptor> matchingDtos = new ArrayList<>();
            String compositeProperties = composeKeyFromProperties(properties);
            if (nonVolatilePropertiesToOid.containsKey(compositeProperties)) {
                for (Long oid : nonVolatilePropertiesToOid.get(compositeProperties)) {
                    if (oidToIdentity.containsKey(oid)) {
                        matchingDtos.add(oidToIdentity.get(oid).getDescriptor());
                    } else {
                        logger.error("OID contained in the nonVolatileProperties cache is not "
                            + "contained in the oid to dto mapping");
                    }
                }
            }
            return matchingDtos;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void report() {
            MemoryMeasurement oidToIdentityMeasure = MemoryMeasurer.measure(oidToIdentity);
            MemoryMeasurement identifyintToOidMeasure =
                MemoryMeasurer.measure(identifyingPropertiesToOid);
            MemoryMeasurement nonVolatilePropsMeasure = MemoryMeasurer.measure(nonVolatilePropertiesToOid);

            logger.debug(String.format("Identity cache size report:\n oidToIdentity size: {} \n "
                    + "identifyingPropertiesToOid size: {} \n nonVolatileProperties cache size: {} \n"),
                oidToIdentityMeasure.toString(), identifyintToOidMeasure.toString(), nonVolatilePropsMeasure.toString());
        }

        private List<PropertyDescriptor> extractNonVolatileProperties(IdentityRecord identityRecord) {
            final ServiceEntityIdentityMetadataStore probeMetadata =
                Objects.requireNonNull(perProbeMetadata.get(identityRecord.getProbeId()));
            ServiceEntityIdentityMetadata entityMetadata =
                probeMetadata.getMetadata(identityRecord.getEntityType());
            List<PropertyDescriptor> identifyingProperties =
                new ArrayList<>(identityRecord.getDescriptor().getIdentifyingProperties());
            List<PropertyDescriptor> nonVolatileProperties = new ArrayList<>();
            for (ServiceEntityProperty property : entityMetadata.getNonVolatileProperties()) {
                Optional<PropertyDescriptor> optionalPropertyDescriptor =
                    findPropertyByRank(identifyingProperties, property.groupId);
                optionalPropertyDescriptor.ifPresent(nonVolatileProperties::add);
            }
            return  nonVolatileProperties;
        }

        private Optional<PropertyDescriptor> findPropertyByRank(List<PropertyDescriptor> propertyDescriptors,
                                                                int rank) {
            return propertyDescriptors.stream().filter(p -> p.getPropertyTypeRank() == rank).findFirst();
        }
    }

    /**
     * This cache exists to support older diags where there are only entity descriptors and not
     * identity records. Without an identity record is impossible to distinguish between non
     * volatile and volatile properties, and for this reason it can't support an optimized cache.
     * Getting an entity by non volatile properties involves iterating through all the entities
     * in the environment. This cache should only be used by loaded environment with older diags.
     */
    public static class DescriptorsBasedCache implements IdentityCache {

        /**
         * Map from the combined identifying properties to OID.
         */
        private final Map<String, Long> identifyingPropertiesToOid = new Object2LongOpenHashMap<>();

        private final Logger logger =
            LogManager.getLogger(IdentityServiceInMemoryUnderlyingStore.class);

        private  Map<Long, EntityInMemoryProxyDescriptor> oidToDescriptor;

        /**
         * Builds a DescriptorsBasedCache.
         */
        public DescriptorsBasedCache() {
            oidToDescriptor = Collections.synchronizedMap(new Long2ObjectOpenHashMap<>());
        }

        @Override
        public EntityInMemoryProxyDescriptor addIdentityRecord(IdentityRecord identityRecord) {
            identifyingPropertiesToOid.put(identityRecord.getDescriptor().getKey(), identityRecord.getDescriptor().getOID());
            return oidToDescriptor.put(identityRecord.getDescriptor().getOID(),
                    identityRecord.getDescriptor());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public  boolean containsKey(long oid) {
            return oidToDescriptor.containsKey(oid);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public EntityInMemoryProxyDescriptor remove(Long oid) {
            EntityInMemoryProxyDescriptor existingDescriptor = oidToDescriptor.remove(oid);
            if (existingDescriptor != null) {
                identifyingPropertiesToOid.remove(existingDescriptor.getKey());
                return existingDescriptor;
            }
            return null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public EntityInMemoryProxyDescriptor get(Long oid) {
            return oidToDescriptor.get(oid);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void toJson(@Nonnull final Writer writer) {
            final EntityInMemoryProxyDescriptorConverter converter =
                new EntityInMemoryProxyDescriptorConverter();
            final GsonBuilder builder = new GsonBuilder();
            builder.registerTypeAdapter(EntityInMemoryProxyDescriptor.class,
                (JsonSerializer<EntityInMemoryProxyDescriptor>)(descriptor, type, jsonSerializationContext) -> new JsonPrimitive(converter.to(descriptor)));
            builder.registerTypeAdapter(EntityInMemoryProxyDescriptor.class,
                (JsonDeserializer<EntityInMemoryProxyDescriptor>)(json, type, context) -> converter.from(json.getAsJsonPrimitive().getAsString()));
            builder.create().toJson(oidToDescriptor.values(), writer);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int size() {
            return oidToDescriptor.keySet().size();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void clear() {
            oidToDescriptor.clear();
            identifyingPropertiesToOid.clear();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public EntityInMemoryProxyDescriptor addDescriptor(EntityInMemoryProxyDescriptor descriptor) {
            return oidToDescriptor.put(descriptor.getOID(), descriptor);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getOidByIdentifyingProperties(@Nonnull final List<PropertyDescriptor> properties) {
            return identifyingPropertiesToOid.getOrDefault(composeKeyFromProperties(properties),
                IdentityService.INVALID_OID);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public List<EntityInMemoryProxyDescriptor> getDtosByNonVolatileProperties(@Nonnull final List<PropertyDescriptor> properties) {
            List<EntityInMemoryProxyDescriptor> array = new ArrayList<>();

            for (EntityInMemoryProxyDescriptor desc : oidToDescriptor.values()) {
                if (desc.containsAll(properties)) {
                    array.add(desc);
                }
            }
            return array;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void report() {
            MemoryMeasurement oidToIdentityMeasure = MemoryMeasurer.measure(oidToDescriptor);
            MemoryMeasurement identifyingPropertiesToOidMeasure = MemoryMeasurer.measure(identifyingPropertiesToOid);

            logger.debug(String.format("Identity cache size report:\n oidToDescriptor size: " + "{} "
                    + "\n identifyingPropertiesToOid size: {} \n"),
                oidToIdentityMeasure.toString(), identifyingPropertiesToOidMeasure.toString());
        }
    }
}
