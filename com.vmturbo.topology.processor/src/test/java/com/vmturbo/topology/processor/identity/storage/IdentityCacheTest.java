package com.vmturbo.topology.processor.identity.storage;

import static junitparams.JUnitParamsRunner.$;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadataStore;
import com.vmturbo.topology.processor.identity.storage.IdentityCaches.DescriptorsBasedCache;
import com.vmturbo.topology.processor.identity.storage.IdentityCaches.IdentityCache;
import com.vmturbo.topology.processor.identity.storage.IdentityCaches.IdentityRecordsBasedCache;

/**
 * Class to test interactions with the identity caches implementations. Each test is run for all
 * the identity cache implementations, all the methods are supposed to return the same results
 * for each implementation, beside the conversions from and to json.
 */
@RunWith(JUnitParamsRunner.class)
public class IdentityCacheTest {
    long probeId = 1L;

    Object[] generateTestData() {
        ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata = new ConcurrentHashMap<>();
        perProbeMetadata.put(probeId,
            new ServiceEntityIdentityMetadataStore(Collections.singletonList(EntityIdentityMetadata
                .newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE).build())));
        IdentityRecordsBasedCache identityRecordsBasedCache = new IdentityRecordsBasedCache(perProbeMetadata);
        DescriptorsBasedCache descriptorsBasedCache =
            new DescriptorsBasedCache();
        return $(
            $(identityRecordsBasedCache),
            $(descriptorsBasedCache));
    }

    /**
     * Add an identity record to the cache. Makes sure it's added to all the internal mappings
     * inside the cache.
     * @param identityCache the instance of the identity cache
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testAddIdentityRecordAndRetrieve(IdentityCache identityCache) {
        final long oid = 1L;

        IdentityRecordWithProperties identityRecordWithProperties = createIdentityRecord(oid, probeId,
            EntityType.VIRTUAL_MACHINE);

        EntityInMemoryProxyDescriptor addedDescriptor =
            identityCache.addIdentityRecord(identityRecordWithProperties.getIdentityRecord());

        Assert.assertNull(addedDescriptor);
        List<EntityInMemoryProxyDescriptor> matchingDescriptorsByNonVolatile =
            identityCache.getDtosByNonVolatileProperties(identityRecordWithProperties.getNonVolatileProperties());

        Assert.assertEquals(1,
            matchingDescriptorsByNonVolatile.size());
        Assert.assertEquals(identityRecordWithProperties.getIdentityRecord().getDescriptor(),
            matchingDescriptorsByNonVolatile.get(0));

        long matchingId =
            identityCache.getOidByIdentifyingProperties(identityRecordWithProperties.getIdentifyingProperties());
        Assert.assertEquals(oid, matchingId);

        addedDescriptor =
            identityCache.get(oid);

        Assert.assertEquals(addedDescriptor,
            identityRecordWithProperties.identityRecord.getDescriptor());
    }

    /**
     * Tests that the contains key method works.
     * @param identityCache the instance of the identity cache
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testContainsKey(IdentityCache identityCache) {
        final long oid = 1L;
        IdentityRecordWithProperties identityRecordWithProperties = createIdentityRecord(oid, probeId,
            EntityType.VIRTUAL_MACHINE);

        Assert.assertFalse(identityCache.containsKey(oid));

        identityCache.addIdentityRecord(identityRecordWithProperties.getIdentityRecord());

        Assert.assertTrue(identityCache.containsKey(oid));
    }

    /**
     * Tests that we can successfully remove records from the cache and all the underlying data
     * structures.
     * @param identityCache the instance of the identity cache
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testRemove(IdentityCache identityCache) {
        final long oid = 1L;
        IdentityRecordWithProperties identityRecordWithProperties = createIdentityRecord(oid,
            probeId,
            EntityType.VIRTUAL_MACHINE);

        identityCache.addIdentityRecord(identityRecordWithProperties.getIdentityRecord());

        Assert.assertTrue(identityCache.containsKey(oid));

        identityCache.remove(oid);

        Assert.assertFalse(identityCache.containsKey(oid));
        Assert.assertEquals(Collections.emptyList(),
            identityCache.getDtosByNonVolatileProperties(identityRecordWithProperties.getNonVolatileProperties()));

        long matchingId =
            identityCache.getOidByIdentifyingProperties(identityRecordWithProperties.getIdentifyingProperties());
        Assert.assertEquals(IdentityService.INVALID_OID, matchingId);
    }

    /**
     * Tests that we can successfully clear records from the cache and all the underlying data
     * structures.
     * @param identityCache the instance of the identity cache
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testClear(IdentityCache identityCache) {
        final long oid = 1L;
        IdentityRecordWithProperties identityRecordWithProperties = createIdentityRecord(oid,
            probeId,
            EntityType.VIRTUAL_MACHINE);

        identityCache.addIdentityRecord(identityRecordWithProperties.getIdentityRecord());

        Assert.assertTrue(identityCache.containsKey(oid));

        identityCache.clear();

        Assert.assertFalse(identityCache.containsKey(oid));
        Assert.assertEquals(Collections.emptyList(),
            identityCache.getDtosByNonVolatileProperties(identityRecordWithProperties.getNonVolatileProperties()));

        long matchingId =
            identityCache.getOidByIdentifyingProperties(identityRecordWithProperties.getIdentifyingProperties());
        Assert.assertEquals(IdentityService.INVALID_OID, matchingId);
    }

    /**
     * Tests that we can correctly reload the records from the JSON file generated in the diags,
     * and correctly serialize them.
     * This is the only place where the two caches implementations differ. The
     * {@link IdentityRecordsBasedCache} will read {@link IdentityRecord}, while the
     * {@link DescriptorsBasedCache} will read {@link EntityInMemoryProxyDescriptor}.
     * @param identityCache the instance of the identity cache
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testFromToJson(IdentityCache identityCache) {
        final long oid = 1L;
        IdentityRecordWithProperties identityRecordWithProperties = createIdentityRecord(oid,
            probeId,
            EntityType.VIRTUAL_MACHINE);
        final StringWriter writer = new StringWriter();
        identityCache.addIdentityRecord(identityRecordWithProperties.getIdentityRecord());
        identityCache.toJson(writer);
        EntityInMemoryProxyDescriptor descriptor;

        if (identityCache instanceof IdentityRecordsBasedCache) {
            List<IdentityRecord> identityRecords = constructGson()
                .fromJson(writer.toString(), new TypeToken<List<IdentityRecord>>() {
                }.getType());

            Assert.assertEquals(1, identityRecords.size());
            Assert.assertEquals(identityRecordWithProperties.getIdentityRecord().getEntityType(),
                (identityRecords.get(0)).getEntityType());
            Assert.assertEquals(identityRecordWithProperties.getIdentityRecord().getProbeId(),
                (identityRecords.get(0)).getProbeId());
            descriptor = identityRecords.get(0).getDescriptor();

        } else {
            final List<EntityInMemoryProxyDescriptor> descriptors = constructGson()
                .fromJson(writer.toString(), new TypeToken<List<EntityInMemoryProxyDescriptor>>() {
                }.getType());
            descriptor = descriptors.get(0);
        }

        for (PropertyDescriptor property : identityRecordWithProperties.getIdentifyingProperties()) {
            Assert.assertTrue((descriptor.getIdentifyingProperties().contains(property)));
        }
    }

    private IdentityRecordWithProperties createIdentityRecord(long oid, long probeId, EntityType entityType) {
        final PropertyDescriptor nonVolatileProperty = new PropertyDescriptorImpl("myId", 1);
        final PropertyDescriptor volatileProperty = new PropertyDescriptorImpl("myInstanceName", 2);
        final List<PropertyDescriptor> identifyingProperties = new ArrayList<>();

        identifyingProperties.add(nonVolatileProperty);
        identifyingProperties.add(volatileProperty);
        IdentityRecord identityRecord =  new IdentityRecord(entityType,
            new EntityInMemoryProxyDescriptor(oid,
                identifyingProperties,
                Collections.EMPTY_LIST), probeId);
        return new IdentityRecordWithProperties(identityRecord,
            Arrays.asList(nonVolatileProperty), Arrays.asList(volatileProperty));
    }

    private Gson constructGson() {
        final EntityInMemoryProxyDescriptorConverter converter =
            new EntityInMemoryProxyDescriptorConverter();
        final GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(EntityInMemoryProxyDescriptor.class,
            (JsonSerializer<EntityInMemoryProxyDescriptor>)(descriptor, type, jsonSerializationContext) -> new JsonPrimitive(converter.to(descriptor)));
        builder.registerTypeAdapter(EntityInMemoryProxyDescriptor.class,
            (JsonDeserializer<EntityInMemoryProxyDescriptor>)(json, type, context) -> converter.from(json.getAsJsonPrimitive().getAsString()));
        return builder.create();
    }

    /**
     * Class to facilitate the creation and interaction with {@link IdentityRecord} and their
     * volatile, non volatile and identifying properties.
     */
    private class IdentityRecordWithProperties {
         private List<PropertyDescriptor> nonVolatileProperties;
         private List<PropertyDescriptor> volatileProperties;
         private IdentityRecord identityRecord;

        IdentityRecordWithProperties(IdentityRecord identityRecord,
                                     List<PropertyDescriptor> nonVolatileProperties,
                                     List<PropertyDescriptor> volatileProperties) {
            this.nonVolatileProperties = nonVolatileProperties;
            this.volatileProperties = volatileProperties;
            this.identityRecord = identityRecord;
        }

        public List<PropertyDescriptor> getNonVolatileProperties() {
            return nonVolatileProperties;
        }

        public List<PropertyDescriptor> getIdentifyingProperties() {
            return Stream.concat(nonVolatileProperties.stream(), volatileProperties.stream())
                .collect(Collectors.toList());
        }

        public IdentityRecord getIdentityRecord() {
            return identityRecord;
        }
    }

}
