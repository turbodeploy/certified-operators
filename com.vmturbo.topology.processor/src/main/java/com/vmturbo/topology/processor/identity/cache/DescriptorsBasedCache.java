package com.vmturbo.topology.processor.identity.cache;

import static com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore.composeKeyFromProperties;

import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.memory.MemoryMeasurer;
import com.vmturbo.common.protobuf.memory.MemoryMeasurer.MemoryMeasurement;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.storage.EntityInMemoryProxyDescriptor;
import com.vmturbo.topology.processor.identity.storage.EntityInMemoryProxyDescriptorConverter;
import com.vmturbo.topology.processor.identity.storage.IdentityRecord;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;

/**
 * This cache exists to support older diags where there are only entity descriptors and not
 * identity records. Without an identity record is impossible to distinguish between non
 * volatile and volatile properties, and for this reason it can't support an optimized cache.
 * Getting an entity by non volatile properties involves iterating through all the entities
 * in the environment. This cache should only be used by loaded environment with older diags.
 */
public class DescriptorsBasedCache implements IdentityCache {

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
    @Nullable
    public EntityInMemoryProxyDescriptor addIdentityRecord(IdentityRecord identityRecord) {
        return addIdentityRecord(identityRecord, null);
    }

    @Override
    @Nullable
    public EntityInMemoryProxyDescriptor addIdentityRecord(IdentityRecord identityRecord, LoadReport report) {
        identifyingPropertiesToOid.put(identityRecord.getDescriptor().getKey(), identityRecord.getDescriptor().getOID());
        return oidToDescriptor.put(identityRecord.getDescriptor().getOID(),
                identityRecord.getDescriptor());
    }

    @Override
    public  boolean containsKey(long oid) {
        return oidToDescriptor.containsKey(oid);
    }

    @Override
    public int remove(Set<Long> oidsToRemove) {
        int removedOids = 0;
        for (Long oid : oidsToRemove) {
            if (remove(oid) != null) {
                removedOids += 1;
            }
        }
        return removedOids;
    }

    private EntityInMemoryProxyDescriptor remove(Long oid) {
        EntityInMemoryProxyDescriptor existingDescriptor = oidToDescriptor.remove(oid);
        if (existingDescriptor != null) {
            identifyingPropertiesToOid.remove(existingDescriptor.getKey());
            return existingDescriptor;
        }
        return null;
    }

    @Override
    public EntityInMemoryProxyDescriptor get(Long oid) {
        return oidToDescriptor.get(oid);
    }

    @Override
    public Set<Long> getOids() {
        LongSet oids = new LongOpenHashSet();
        oids.addAll(oidToDescriptor.keySet());
        return oids;
    }

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

    @Override
    public void clear() {
        oidToDescriptor.clear();
        identifyingPropertiesToOid.clear();
    }

    @Override
    public EntityInMemoryProxyDescriptor addDescriptor(EntityInMemoryProxyDescriptor descriptor) {
        return oidToDescriptor.put(descriptor.getOID(), descriptor);
    }

    @Override
    public long getOidByIdentifyingProperties(@Nonnull List<PropertyDescriptor> nonVolatileProperties,
            @Nonnull List<PropertyDescriptor> volatileProperties) {
        List<PropertyDescriptor> identifyingProps = Stream.concat(nonVolatileProperties.stream(), volatileProperties.stream())
                .collect(Collectors.toList());
        return identifyingPropertiesToOid.getOrDefault(composeKeyFromProperties(identifyingProps), IdentityService.INVALID_OID);
    }

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

    @Override
    public void report() {
        MemoryMeasurement oidToIdentityMeasure = MemoryMeasurer.measure(oidToDescriptor);
        MemoryMeasurement identifyingPropertiesToOidMeasure = MemoryMeasurer.measure(identifyingPropertiesToOid);

        logger.debug(String.format("Identity cache size report:\n oidToDescriptor size: " + "{} "
                        + "\n identifyingPropertiesToOid size: {} \n"),
                oidToIdentityMeasure.toString(), identifyingPropertiesToOidMeasure.toString());
    }
}
