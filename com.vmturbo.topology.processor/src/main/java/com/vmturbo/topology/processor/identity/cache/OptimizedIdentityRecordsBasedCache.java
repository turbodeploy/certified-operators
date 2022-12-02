package com.vmturbo.topology.processor.identity.cache;

import static com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore.composeKeyFromProperties;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.memory.MemoryMeasurer;
import com.vmturbo.common.protobuf.memory.MemoryMeasurer.MemoryMeasurement;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadata;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadataStore;
import com.vmturbo.topology.processor.identity.storage.EntityInMemoryProxyDescriptor;
import com.vmturbo.topology.processor.identity.storage.EntityInMemoryProxyDescriptorConverter;
import com.vmturbo.topology.processor.identity.storage.IdentityRecord;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;

/**
 * Optimized version of a {@link DescriptorsBasedCache}. All the operations can be performed
 * in constant time.
 */
public class OptimizedIdentityRecordsBasedCache implements IdentityCache {

    private final Map<Long, IdentityRecordWithProperties> oidToIdentity;

    private final ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata;

    private final Logger logger =
            LogManager.getLogger(IdentityServiceInMemoryUnderlyingStore.class);

    private final Map<String, Map<String, Long>>
            identifyingPropertiesTable = new HashMap<>();

    /**
     * Get a OptimizedIdentityRecordsBasedCache.
     * @param perProbeMetadata map containing probe it to probe metadata
     */
    public OptimizedIdentityRecordsBasedCache(@Nonnull ConcurrentMap<Long,
            ServiceEntityIdentityMetadataStore> perProbeMetadata) {
        this.perProbeMetadata = perProbeMetadata;
        this.oidToIdentity = new Long2ObjectOpenHashMap<>();
    }

    @Override
    @Nullable
    public synchronized EntityInMemoryProxyDescriptor addIdentityRecord(
            IdentityRecord identityRecord) {
        return addIdentityRecord(identityRecord, null);
    }

    @Override
    @Nullable
    public synchronized EntityInMemoryProxyDescriptor addIdentityRecord(
            IdentityRecord identityRecord, LoadReport loadReport) {
        final IdentifyingProperties identifyingProperties = extractIdentifyingProperties(identityRecord, loadReport);
        final long recordOid = identityRecord.getDescriptor().getOID();
        final String nonVolatileProperties = identifyingProperties.getNonVolatilesProperties();
        final String volatileProperties = identifyingProperties.getVolatilesProperties();
        Map<String, Long> volatileMap = identifyingPropertiesTable.computeIfAbsent(nonVolatileProperties, k -> new HashMap<>());
        volatileMap.put(volatileProperties, recordOid);
        IdentityRecord existingRecord =
                oidToIdentity.put(recordOid,
                        new IdentityRecordWithProperties(identityRecord.getEntityType(), identityRecord.getDescriptor(), identityRecord.getProbeId(),
                                nonVolatileProperties, volatileProperties));
        return existingRecord != null ? existingRecord.getDescriptor() : null;
    }

    @Override
    public synchronized boolean containsKey(long oid) {
        return oidToIdentity.containsKey(oid);
    }

    @Override
    public synchronized int remove(Set<Long> oidsToRemove) {
        int removedOids = 0;
        logger.info("Performing bulk removal of {} oids", oidsToRemove.size());
        final Stopwatch sw = Stopwatch.createStarted();
        for (Long oid: oidsToRemove) {
            if (remove(oid) != null) {
                removedOids += 1;
            }
        }
        oidToIdentity.keySet().removeAll(oidsToRemove);
        logger.info("Done performing bulk removal of {} oids in {}", oidsToRemove.size(),
                sw);
        return removedOids;
    }

    private EntityInMemoryProxyDescriptor remove(Long oid) {
        IdentityRecordWithProperties existingRecord = oidToIdentity.remove(oid);
        if (existingRecord != null) {
            identifyingPropertiesTable.computeIfPresent(existingRecord.getNonVolatilesProperties(),
                    (k, nonVolatileMap) -> nonVolatileMap.remove(existingRecord.getVolatilesProperties(), oid) && nonVolatileMap.isEmpty() ? null : nonVolatileMap);
            return existingRecord.getDescriptor();
        }
        return null;
    }

    @Override
    public synchronized EntityInMemoryProxyDescriptor get(Long oid) {
        IdentityRecord existingRecord = oidToIdentity.get(oid);
        return existingRecord != null ? existingRecord.getDescriptor() : null;
    }

    @Override
    public synchronized void toJson(@Nonnull final Writer writer) {
        final EntityInMemoryProxyDescriptorConverter converter =
                new EntityInMemoryProxyDescriptorConverter();
        final GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(EntityInMemoryProxyDescriptor.class,
                (JsonSerializer<EntityInMemoryProxyDescriptor>)(descriptor, type, jsonSerializationContext) -> new JsonPrimitive(converter.to(descriptor)));
        Gson gson =  builder.create();
        try {
            writer.append("[");
            String separator = "";
            for (Object record : oidToIdentity.values()) {
                writer.append(separator + gson.toJson(record));
                separator = ",";
            }
            writer.append("]");
        } catch (IOException e) {
            logger.error("Failed to append oidToIdentity data to dump file. Error: {}", e.getMessage());
        }
    }

    @Override
    public synchronized void clear() {
        oidToIdentity.clear();
        identifyingPropertiesTable.clear();
    }

    @Override
    public synchronized EntityInMemoryProxyDescriptor addDescriptor(EntityInMemoryProxyDescriptor descriptor) {
        logger.error("This method is not supported for this type of cache");
        return null;
    }

    @Override
    public synchronized long getOidByIdentifyingProperties(
            @NotNull List<PropertyDescriptor> nonVolatileProperties,
            @NotNull List<PropertyDescriptor> volatileProperties) {
        final String serializedNonVolatileProps = composeKeyFromProperties(nonVolatileProperties);
        final String serializedVolatileProps = composeKeyFromProperties(volatileProperties);
        if (identifyingPropertiesTable.containsKey(serializedNonVolatileProps)) {
            return identifyingPropertiesTable.get(serializedNonVolatileProps).getOrDefault(serializedVolatileProps, IdentityService.INVALID_OID);
        }
        return IdentityService.INVALID_OID;
    }

    @Override
    public synchronized List<EntityInMemoryProxyDescriptor> getDtosByNonVolatileProperties(
            @Nonnull final List<PropertyDescriptor> nonVolatileProperties) {
        List<EntityInMemoryProxyDescriptor> matchingDtos = new ArrayList<>();
        final String serializedNonVolatileProps = composeKeyFromProperties(nonVolatileProperties);
        if (identifyingPropertiesTable.containsKey(serializedNonVolatileProps)) {
            Collection<Long> matchingOids = identifyingPropertiesTable.get(
                    serializedNonVolatileProps).values();
            for (Long oid : matchingOids) {
                if (oidToIdentity.containsKey(oid)) {
                    matchingDtos.add(oidToIdentity.get(oid).getDescriptor());
                } else {
                    logger.error("OID {} contained in the identifyingPropertiesTable is not contained in the oid to dto mapping", oid);
                }
            }
        }
        return matchingDtos;
    }

    @Override
    public synchronized void report() {
        MemoryMeasurement oidToIdentityMeasure = MemoryMeasurer.measure(oidToIdentity);
        MemoryMeasurement identifyingPropsTableMeasure = MemoryMeasurer.measure(
                identifyingPropertiesTable);

        logger.info("Identity cache size report:\n oidToIdentity size: {} \n "
                        + "identifyingPropertiesTable size: {} for oids {}",
                oidToIdentityMeasure.toString(), identifyingPropsTableMeasure.toString(), oidToIdentity.size());
    }

    @Override
    public synchronized Set<Long> getOids() {
        LongSet oids = new LongOpenHashSet();
        oids.addAll(oidToIdentity.keySet());
        return oids;
    }

    private IdentifyingProperties extractIdentifyingProperties(IdentityRecord identityRecord,
            LoadReport report) {
        List<PropertyDescriptor> nonVolatileProperties = new ArrayList<>();
        List<PropertyDescriptor> volatileProperties = new ArrayList<>();
        if (perProbeMetadata.containsKey(identityRecord.getProbeId())) {
            final ServiceEntityIdentityMetadataStore probeMetadata = perProbeMetadata.get(
                    identityRecord.getProbeId());
            ServiceEntityIdentityMetadata entityMetadata = probeMetadata.getMetadata(
                    identityRecord.getEntityType());
            if (entityMetadata != null) {
                List<PropertyDescriptor> identifyingProperties = new ArrayList<>(
                        identityRecord.getDescriptor().getIdentifyingProperties());
                if (identifyingProperties.size() == 0) {
                    if (report != null) {
                        report.missingIdProps(identityRecord);
                    }
                }
                Set<Integer> nonVolatileRanks = new HashSet<>(entityMetadata.getNonVolatilePropertyRanks());
                Collection<Integer> volatileRanks = new HashSet<>(entityMetadata.getVolatilePropertyRanks());

                for (PropertyDescriptor identifyingProp : identifyingProperties) {
                    if (nonVolatileRanks.contains(identifyingProp.getPropertyTypeRank())) {
                        nonVolatileProperties.add(identifyingProp);
                    } else if (volatileRanks.contains(identifyingProp.getPropertyTypeRank())) {
                        volatileProperties.add(identifyingProp);
                    }
                }
            } else {
                if (report != null) {
                    report.missingEntityMetadata(identityRecord);
                }
            }
        } else {
            if (report != null) {
                report.missingProbeMetadata(identityRecord);
            }
        }
        return new IdentifyingProperties(nonVolatileProperties, volatileProperties);
    }

    /**
     * Utility class that contains volatile and non volatile properties and compose them into
     * strings to facilitate the keys creation for the identifyingPropertiesTable.
     */
    private static class IdentifyingProperties {
        private final List<PropertyDescriptor> nonVolatilesProperties;
        private final List<PropertyDescriptor> volatilesProperties;

        private IdentifyingProperties(List<PropertyDescriptor> nonVolatilesProperties,
                List<PropertyDescriptor> volatilesProperties) {
            this.nonVolatilesProperties = nonVolatilesProperties;
            this.volatilesProperties = volatilesProperties;
        }

        public String getNonVolatilesProperties() {
            return composeKeyFromProperties(nonVolatilesProperties);
        }

        public String getVolatilesProperties() {
            return composeKeyFromProperties(volatilesProperties);
        }
    }

    /**
     * Class that facilitates caching the associated nonvolatile and volatile properties for an
     * {@link IdentityRecord}.
     */
    private static class IdentityRecordWithProperties extends IdentityRecord {

        private final String nonVolatilesProperties;
        private final String volatilesProperties;


        private IdentityRecordWithProperties(@NotNull EntityType entityType,
                @NotNull EntityInMemoryProxyDescriptor memoryDescriptor, long probeId, String nonVolatilesProperties,
                String volatilesProperties) {
            super(entityType, memoryDescriptor, probeId);
            this.nonVolatilesProperties = nonVolatilesProperties;
            this.volatilesProperties = volatilesProperties;
        }

        public String getNonVolatilesProperties() {
            return nonVolatilesProperties;
        }

        public String getVolatilesProperties() {
            return volatilesProperties;
        }
    }
}

