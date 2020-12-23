package com.vmturbo.topology.processor.identity.storage;

import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.identity.EntityDescriptor;
import com.vmturbo.topology.processor.identity.EntityMetadataDescriptor;
import com.vmturbo.topology.processor.identity.EntryData;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.IdentityServiceStoreOperationException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.identity.IdentityWrongSetException;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadataStore;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;
import com.vmturbo.topology.processor.identity.storage.IdentityCaches.DescriptorsBasedCache;
import com.vmturbo.topology.processor.identity.storage.IdentityCaches.IdentityCache;
import com.vmturbo.topology.processor.identity.storage.IdentityCaches.IdentityRecordsBasedCache;

/**
 * The VMTIdentityServiceInMemoryUnderlyingStore implements the in-memory underlying store. The
 * different entity types might potentially have the same set of properties. This is handled by the
 * entity subtype being a volatile property. The responsibility for supplying it lies with the
 * probe. This is because the entity may potentially chane its type. For example, the VM migrates
 * from VC to Amazon.
 *
 * The {@link IdentityServiceInMemoryUnderlyingStore} backs up all assigned OIDs in the injected
 * {@link IdentityDatabaseStore}, and restores them asynchronously upon startup. All methods
 * will throw {@link IdentityUninitializedException} until the initial restoration of IDs from the
 * database completes. We do this because we do NOT want to re-assign an OID that's already
 * been assigned, or assign a new OID to an object that already has an OID - it will have
 * wide-ranging effects throughout the system.
 */
@NotThreadSafe public class IdentityServiceInMemoryUnderlyingStore
        implements IdentityServiceUnderlyingStore {

    private static final Logger LOGGER =
        LogManager.getLogger(IdentityServiceInMemoryUnderlyingStore.class);

    /**
     * The rank and property separator for the String representation.
     */
    private static final char PROPERTY_STRING_SEP = ':';

    /**
     * Whether or not the identity store has been initialized.
     */
    @GuardedBy("initializationLock")
    private boolean initialized = false;

    private final Object initializationLock = new Object();

    private final IdentityDatabaseStore identityDatabaseStore;

    private long loadIdsInterval = 10;

    private TimeUnit loadIdsTimeUnit = TimeUnit.SECONDS;

    private final int initializationTimeoutMin;

    ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata;

    private IdentityCache identityCache;

    public IdentityServiceInMemoryUnderlyingStore(
            @Nonnull final IdentityDatabaseStore identityDatabaseStore,
            final int initializationTimeoutMin,
            final long loadIdsInterval,
            final TimeUnit loadIdsTimeUnit,
            ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata,
            boolean useIdentityRecordsCache) {
        this.identityDatabaseStore = identityDatabaseStore;
        this.initializationTimeoutMin = initializationTimeoutMin;
        this.perProbeMetadata = perProbeMetadata;
        // We do the initialization asynchronously so the rest of Spring initialization can
        // complete, but all methods should throw an exception until the
        // initialization completes so that we don't assign new IDs to entities that already
        // have IDs in the system.
        //
        // The intention behind doing this asynchronously is to allow partial functionality
        // of the Topology Processor - e.g. probe registration, target management, running
        // plans on old topologies - even if the Identity Service is not initialized.
        this.loadIdsInterval = loadIdsInterval;
        this.loadIdsTimeUnit = loadIdsTimeUnit;
        initializeIdentityCache(perProbeMetadata, useIdentityRecordsCache);
    }

    @VisibleForTesting
    public IdentityServiceInMemoryUnderlyingStore(
        @Nonnull final IdentityDatabaseStore identityDatabaseStore,
        final int initializationTimeoutMin,
        ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata) {
        this.identityDatabaseStore = identityDatabaseStore;
        this.initializationTimeoutMin = initializationTimeoutMin;
        this.identityCache = new IdentityRecordsBasedCache(perProbeMetadata);
        addRestoredIds(Collections.emptySet());
    }

    /**
     * Initialize the store.
     */
    public void initialize() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(
            new SavedIdsLoader(identityDatabaseStore, loadIdsInterval, loadIdsTimeUnit,
                this::addRestoredIds));
        executor.shutdown();
        try {
            boolean success = executor.awaitTermination(initializationTimeoutMin, TimeUnit.MINUTES);
            if (!success) {
                LOGGER.error("Failed to initialize the store due to time out");
            }
        } catch (InterruptedException e) {
            LOGGER.error("Identity store initialization interrupted", e);
        }
    }

    private static class SavedIdsLoader implements Runnable {

        private IdentityDatabaseStore identityDatabaseStore;
        private final long retryMs;
        private Consumer<Set<IdentityRecord>> onComplete;

        SavedIdsLoader(@Nonnull final IdentityDatabaseStore identityDatabaseStore,
                       final long retryInterval,
                       @Nonnull final TimeUnit retryUnit,
                       Consumer<Set<IdentityRecord>> onComplete) {
            this.identityDatabaseStore = identityDatabaseStore;
            this.retryMs = TimeUnit.MILLISECONDS.convert(retryInterval, retryUnit);
            this.onComplete = onComplete;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Set<IdentityRecord> descriptors =
                            identityDatabaseStore.getDescriptors();
                    onComplete.accept(descriptors);
                    return;
                } catch (IdentityDatabaseException e) {
                    LOGGER.info("Failed to re-load saved IDs from the DB: {}", e.getMessage());
                    try {
                        Thread.sleep(retryMs);
                    } catch (InterruptedException e1) {
                        LOGGER.error("Interrupted while loading saved IDs from the DB.", e1);
                        Thread.currentThread().interrupt();
                        return;
                    }
                } catch (RuntimeException e) {
                    // Despite the unexpected exception, we should continue attempts to re-load.
                    // At the very least, the logspam will make it obvious that something is
                    // seriously wrong.
                    LOGGER.error("Unexpected exception when loading saved IDs from the DB.", e);
                }
            }
        }
    }

    /*
      Update the in-memory cache with the latest entries from the DB.
     */
    public void reloadEntityDescriptors() {

        try {
            synchronized (initializationLock) {
                while (initialized != true) {
                    initializationLock.wait();
                }
                identityCache.clear();
                initialized = false;
            }
            (new SavedIdsLoader(identityDatabaseStore, loadIdsInterval, loadIdsTimeUnit,
                this::addRestoredIds)).run();
        } catch (InterruptedException ie) {
            LOGGER.error("Interrupted while loading saved IDs from the DB.", ie);
            Thread.currentThread().interrupt();
            return;
        }
    }

    /**
     * Wait for the store to be initialized. Threads will wait on this lock until the
     * initialization process, once done, will call the method notifyAll on the initializationLock.
     *
     * @throws InterruptedException if any thread interrupted the current thread before
     * or while the current thread was waiting for a notification.
     */
    public void waitForInitializedStore() throws InterruptedException {
        synchronized (initializationLock) {
            while (!initialized) {
                initializationLock.wait(initializationTimeoutMin * 60000);
            }
        }
    }

    /**
     * Returns the String representation of a property. The correct representation is: {@code
     * <rank>:<value>}
     *
     * @param vpd The property descriptor.
     * @return The String representation of a property.
     */
    @Nonnull
    static String propertyAsString(PropertyDescriptor vpd) {
        StringBuilder sb = new StringBuilder();
        sb.append(vpd.getPropertyTypeRank());
        sb.append(PROPERTY_STRING_SEP);
        sb.append(vpd.getValue());
        return sb.toString();
    }

    /**
     * Parses the property descriptor as composed by the {@link #propertyAsString}. We assume that
     * the String will have the correct representation: {@code <rank>:<value>},
     *
     * @param s The String representation of the property descriptor.
     * @return The property descriptor.
     */
    static PropertyDescriptor parseString(final String s) {
        final int index = s.indexOf(PROPERTY_STRING_SEP);
        if (index == -1) {
            throw new IllegalStateException("Corrupt property: " + s);
        }

        return new PropertyDescriptorImpl(s.substring(index + 1),
            Integer.parseInt(s.substring(0, index)));
    }

    /**
     * Composes the key for the list of properties. We use it for the quick check whether or not the
     * Entity is already here and has a full match. This key will be used to identify the object,
     * so, order of elements put into the result string matters.
     *
     * @param properties The property set.
     * @return The key.
     */
    static @Nonnull String composeKeyFromProperties(
        @Nonnull List<PropertyDescriptor> properties) {
        StringBuilder sb = new StringBuilder();
        for (PropertyDescriptor vpd : properties) {
            sb.append(propertyAsString(vpd));
        }
        return sb.toString();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long lookupByIdentifyingSet(@Nonnull EntityMetadataDescriptor metadataDescriptor,
                                       @Nonnull List<PropertyDescriptor> properties)
        throws IdentityServiceStoreOperationException, IdentityUninitializedException {
        checkInitialized();
        return identityCache.getOidByIdentifyingProperties(properties);
    }

    @VisibleForTesting
    void checkInitialized() throws IdentityUninitializedException {
        synchronized (initializationLock) {
            if (!initialized) {
                throw new IdentityUninitializedException();
            }
        }
    }

    private void addRestoredIds(@Nonnull final Set<IdentityRecord>  identityRecordsToProbeId) {
        synchronized (initializationLock) {
            if (!initialized) {
                identityRecordsToProbeId.forEach(identityRecord -> {
                    EntityInMemoryProxyDescriptor descriptor = identityRecord.getDescriptor();
                    EntityInMemoryProxyDescriptor existing =
                        identityCache.addIdentityRecord(identityRecord);
                    if (existing != null && !existing.equals(descriptor)) {
                        // If we don't initialize the IdentityService until the restoration from the
                        // database completes, then this should never happen.
                        LOGGER.error("The ID {} was associated with a different set of properties" +
                            " than the ones retrieved from the database.", descriptor.getOID());
                    }
                });
                LOGGER.info("Done with the identity cache initialization");
                if (LOGGER.isDebugEnabled()) {
                    identityCache.report();
                }
                setStoreAsInitializedAndNotify();
            }
        }
    }

    /**
     * {@inheritDoc}}
     */
    public void upsertEntries(@Nonnull final Map<Long, EntryData> entryMap)
        throws IdentityServiceStoreOperationException, IdentityUninitializedException {
        checkInitialized();

        List<IdentityRecord> updatedRecords = new ArrayList<>();
        for (final Entry<Long, EntryData> entry : entryMap.entrySet()) {
            try {
                final EntryData entryData = entry.getValue();
                final EntityInMemoryProxyDescriptor vmtPD =
                    new EntityInMemoryProxyDescriptor(entry.getKey(),
                        entryData.getDescriptor(),
                        entryData.getMetadata());
                final EntityInMemoryProxyDescriptor oldPd = identityCache.get(vmtPD.getOID());
                EntityType entityType = entryData.getEntityDTO().get().getEntityType();
                if (!vmtPD.equals(oldPd)) {
                    updatedRecords.add(new IdentityRecord(entityType, vmtPD, entryData.getProbeId()));
                }
            } catch (IdentityWrongSetException e) {
                throw new IdentityServiceStoreOperationException(e);
            }
        }

        try {
            identityDatabaseStore.saveDescriptors(updatedRecords);
            } catch (IdentityDatabaseException e) {
            throw new IdentityServiceStoreOperationException(e);
        }

        // Do this after the database update is successful, to keep the in-memory index from
        // being out-of-date with the database.
        updatedRecords.forEach(record -> identityCache.addIdentityRecord(record));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addEntry(final long oid,
                         @Nonnull final EntityDescriptor descriptor,
                         @Nonnull final EntityMetadataDescriptor metadataDescriptor,
                         @Nonnull final EntityType entityType,
                         final long probeId)
        throws IdentityServiceStoreOperationException, IdentityUninitializedException {
        checkInitialized();
        try {
            final EntityInMemoryProxyDescriptor vmtPD =
                    new EntityInMemoryProxyDescriptor(oid, descriptor, metadataDescriptor);
            IdentityRecord identityRecord = new IdentityRecord(entityType, vmtPD, probeId);
            identityDatabaseStore.saveDescriptors(Collections.singletonList(identityRecord));
            identityCache.addIdentityRecord(identityRecord);
        } catch (IdentityWrongSetException | IdentityDatabaseException e) {
            throw new IdentityServiceStoreOperationException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateEntry(final long oid,
                            @Nonnull final EntityDescriptor descriptor,
                            @Nonnull final EntityMetadataDescriptor metadataDescriptor,
                            @Nonnull final EntityType entityType,
                            final long probeId)
        throws IdentityServiceStoreOperationException, IdentityUninitializedException {

        checkInitialized();

        EntityInMemoryProxyDescriptor existing = identityCache.remove(oid);
        if (existing == null) {
            throw new IllegalStateException(
                "The OID " + oid + " does not correspond to an existing Entity");
        }

        try {
            final EntityInMemoryProxyDescriptor vmtPD =
                    new EntityInMemoryProxyDescriptor(oid, descriptor, metadataDescriptor);
            final IdentityRecord identityRecord = new IdentityRecord(entityType, vmtPD, probeId);
            identityDatabaseStore.saveDescriptors(Collections.singletonList(identityRecord));
            identityCache.addIdentityRecord(identityRecord);
        } catch (IdentityWrongSetException | IdentityDatabaseException e) {
            // Put the old stuff back, since the update failed.
            identityCache.addIdentityRecord(new IdentityRecord(entityType, existing, probeId));
            throw new IdentityServiceStoreOperationException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeEntry(long oid)
        throws IdentityServiceStoreOperationException, IdentityUninitializedException {
        checkInitialized();

        final EntityInMemoryProxyDescriptor vmtPD = identityCache.remove(oid);
        if (vmtPD != null) {
            try {
                identityDatabaseStore.removeDescriptor(oid);
            } catch (IdentityDatabaseException e) {
                throw new IdentityServiceStoreOperationException(e);
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     * @return
     */
    @Nonnull
    public List<EntityInMemoryProxyDescriptor> getDtosByNonVolatileProperties(
        @Nonnull final List<PropertyDescriptor> properties)
            throws IdentityUninitializedException {
        checkInitialized();
        return identityCache.getDtosByNonVolatileProperties(properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsOID(long oid)
        throws IdentityServiceStoreOperationException, IdentityUninitializedException {
        checkInitialized();
        return identityCache.containsKey(oid);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsWithIdentifyingProperties(
        @Nonnull EntityMetadataDescriptor metadataDescriptor,
        @Nonnull List<PropertyDescriptor> properties)
        throws IdentityServiceStoreOperationException, IdentityUninitializedException {
        checkInitialized();
        return lookupByIdentifyingSet(metadataDescriptor, properties) !=
            IdentityService.INVALID_OID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void backup(@Nonnull final Writer writer) {
        identityCache.toJson(writer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restore(@Nonnull final Reader input, @Nonnull Map<Long,
        ServiceEntityIdentityMetadataStore> perProbeMetadata) {
        synchronized (initializationLock) {
            // We don't check if initialized is true or not, because the "restore" overrides
            // whatever was initialized anyway.
            // But we set the initialized flag to true, so that no information gets lost if
            // "restore" happens before ID data is re-loaded.
            initialized = true;
            identityCache.clear();
            List<IdentityRecord> identityRecords = constructGson()
                    .fromJson(input, new TypeToken<List<IdentityRecord>>() {
                    }.getType());
            identityRecords.forEach(identityRecord -> {
                identityCache.addIdentityRecord(identityRecord);
            });
            setStoreAsInitializedAndNotify();
        }
    }

    /**
     * Once the store is initialized we need to notify all the threads that might be waiting on
     * the initializationLock.
     */
    @GuardedBy("initializationLock")
    private void setStoreAsInitializedAndNotify() {
        initialized = true;
        initializationLock.notifyAll();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreOldDiags(@Nonnull  final Reader input) {
        synchronized (initializationLock) {
            initialized = true;
            identityCache.clear();
            final List<EntityInMemoryProxyDescriptor> newOid2Dto = constructGson()
                .fromJson(input, new TypeToken<List<EntityInMemoryProxyDescriptor>>() {
                }.getType());
            identityCache = new DescriptorsBasedCache();
            newOid2Dto.forEach(descriptor -> {
                identityCache.addDescriptor(descriptor);
            });
        }
    }

    /**
     * Inizialize the cache based on the config parameter useIdentityRecordsCache. The
     * {@link IdentityRecordsBasedCache} is a faster cached optimized to lookup entities by non
     * volatile oids. It consumes more memory, and in the first stage it will only be used at
     * customers that are dealing with slow discovery ingestion see OM-63916 for more context.
     *
     * @param perProbeMetadata probes with their metadata
     * @param useIdentityRecordsCache whether to use the {@link IdentityRecordsBasedCache} or
     * {@link DescriptorsBasedCache}
     *
     */
    private void initializeIdentityCache(ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata, boolean useIdentityRecordsCache) {
        if (useIdentityRecordsCache) {
            this.identityCache = new IdentityRecordsBasedCache(perProbeMetadata);
        } else {
            this.identityCache = new DescriptorsBasedCache();
        }
    }

    private Gson constructGson() {
        final EntityInMemoryProxyDescriptorConverter converter =
            new EntityInMemoryProxyDescriptorConverter();
        final GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(EntityInMemoryProxyDescriptor.class,
            (JsonSerializer<EntityInMemoryProxyDescriptor>) (descriptor, type, jsonSerializationContext) -> new JsonPrimitive(converter.to(descriptor)));
        builder.registerTypeAdapter(EntityInMemoryProxyDescriptor.class,
            (JsonDeserializer<EntityInMemoryProxyDescriptor>) (json, type, context) -> converter.from(json.getAsJsonPrimitive().getAsString()));
        return builder.create();
    }
}
