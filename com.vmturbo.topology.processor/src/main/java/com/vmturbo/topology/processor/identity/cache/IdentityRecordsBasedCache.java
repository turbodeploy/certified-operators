package com.vmturbo.topology.processor.identity.cache;

import static com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore.composeKeyFromProperties;

import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.memory.MemoryMeasurer;
import com.vmturbo.common.protobuf.memory.MemoryMeasurer.MemoryMeasurement;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadata;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadataStore;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityProperty;
import com.vmturbo.topology.processor.identity.storage.EntityInMemoryProxyDescriptor;
import com.vmturbo.topology.processor.identity.storage.EntityInMemoryProxyDescriptorConverter;
import com.vmturbo.topology.processor.identity.storage.IdentityRecord;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;

/**
 * Identity cache that works with {@link IdentityRecord}. This cache is able to distinguish between
 * volatile and non volatile properties among entities and is optimized to lookup entities by
 * non volatile properties.
 */
public class IdentityRecordsBasedCache implements IdentityCache {

    private Map<Long, IdentityRecord> oidToIdentity;

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

    @Override
    @Nullable
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

    @Override
    public boolean containsKey(long oid) {
        return oidToIdentity.containsKey(oid);
    }

    @Override
    public int remove(Set<Long> oidsToRemove) {
        int removedOids = 0;
        logger.info("Performing bulk removal of {} oids", oidsToRemove.size());
        final Stopwatch sw = Stopwatch.createStarted();
        for (Long oid : oidsToRemove) {
            if (remove(oid) != null) {
                removedOids += 1;
            }
        }
        oidToIdentity.keySet().removeAll(oidsToRemove);
        logger.info("Done performing bulk removal of {} oids in {} seconds", oidsToRemove.size(),
                sw);
        return removedOids;
    }

    private EntityInMemoryProxyDescriptor remove(Long oid) {
        IdentityRecord existingRecord = oidToIdentity.remove(oid);
        if (existingRecord != null) {
            identifyingPropertiesToOid.remove(existingRecord.getDescriptor().getKey());
            List<PropertyDescriptor> nonVolatileProperties =
                    extractNonVolatileProperties(existingRecord);
            String serializedNonVolatileProps = composeKeyFromProperties(nonVolatileProperties);
            LongArrayList matchingOids = nonVolatilePropertiesToOid.get(serializedNonVolatileProps);
            if (matchingOids != null) {
                LongArrayList filteredOids = new LongArrayList(
                        matchingOids.stream().filter(matchingOid -> !matchingOid.equals(oid)).collect(
                                Collectors.toList()));
                nonVolatilePropertiesToOid.put(serializedNonVolatileProps, filteredOids);
            }
            return existingRecord.getDescriptor();
        }
        return null;
    }

    @Override
    public EntityInMemoryProxyDescriptor get(Long oid) {
        IdentityRecord existingRecord = oidToIdentity.get(oid);
        return existingRecord != null ? existingRecord.getDescriptor() : null;
    }

    @Override
    public void toJson(@Nonnull final Writer writer) {
        final EntityInMemoryProxyDescriptorConverter converter =
                new EntityInMemoryProxyDescriptorConverter();
        final GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(EntityInMemoryProxyDescriptor.class,
                (JsonSerializer<EntityInMemoryProxyDescriptor>)(descriptor, type, jsonSerializationContext) -> new JsonPrimitive(converter.to(descriptor)));
        builder.create().toJson(oidToIdentity.values(), writer);
    }

    @Override
    public void clear() {
        oidToIdentity.clear();
        identifyingPropertiesToOid.clear();
        nonVolatilePropertiesToOid.clear();
    }

    @Override
    public EntityInMemoryProxyDescriptor addDescriptor(EntityInMemoryProxyDescriptor descriptor) {
        logger.error("This method is not supported for this type of cache");
        return null;
    }

    @Override
    public long getOidByIdentifyingProperties(@NotNull List<PropertyDescriptor> nonVolatileProperties,
            @NotNull List<PropertyDescriptor> volatileProperties) {
        List<PropertyDescriptor> identifyingProps = Stream.concat(nonVolatileProperties.stream(), volatileProperties.stream())
                .collect(Collectors.toList());
        return identifyingPropertiesToOid.getOrDefault(composeKeyFromProperties(identifyingProps),
                IdentityService.INVALID_OID);
    }

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

    @Override
    public Set<Long> getOids() {
        LongSet oids = new LongOpenHashSet();
        oids.addAll(oidToIdentity.keySet());
        return oids;
    }

    private List<PropertyDescriptor> extractNonVolatileProperties(IdentityRecord identityRecord) {
        List<PropertyDescriptor> nonVolatileProperties = new ArrayList<>();
        final ServiceEntityIdentityMetadataStore probeMetadata =
                perProbeMetadata.get(identityRecord.getProbeId());
        ServiceEntityIdentityMetadata entityMetadata =
                probeMetadata.getMetadata(identityRecord.getEntityType());
        if (entityMetadata != null) {
            List<PropertyDescriptor> identifyingProperties =
                    new ArrayList<>(identityRecord.getDescriptor().getIdentifyingProperties());
            for (ServiceEntityProperty property : entityMetadata.getNonVolatileProperties()) {
                Optional<PropertyDescriptor> optionalPropertyDescriptor =
                        findPropertyByRank(identifyingProperties, property.groupId);
                optionalPropertyDescriptor.ifPresent(nonVolatileProperties::add);
            }
        } else {
            logger.warn("Could not find entity metadata for the following entity type {}, "
                            + "this can happen if the entity type has been changed on the probe info",
                    identityRecord.getEntityType());
        }
        return nonVolatileProperties;
    }

    private Optional<PropertyDescriptor> findPropertyByRank(List<PropertyDescriptor> propertyDescriptors,
            int rank) {
        return propertyDescriptors.stream().filter(p -> p.getPropertyTypeRank() == rank).findFirst();
    }
}
