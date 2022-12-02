package com.vmturbo.topology.processor.identity.storage;

import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.identity.EntryData;
import com.vmturbo.topology.processor.identity.IdentityServiceStoreOperationException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.cache.DescriptorsBasedCache;
import com.vmturbo.topology.processor.identity.cache.IdentityCache;
import com.vmturbo.topology.processor.identity.cache.IdentityCache.LoadReport;
import com.vmturbo.topology.processor.identity.cache.OptimizedIdentityRecordsBasedCache;
import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadataStore;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;

/**
 * The VMTIdentityServiceInMemoryUnderlyingStore implements the in-memory underlying store. The
 * different entity types might potentially have the same set of properties. This is handled by the
 * entity subtype being a volatile property. The responsibility for supplying it lies with the
 * probe. This is because the entity may potentially chane its type. For example, the VM migrates
 * from VC to Amazon.
 *
 * <p>The {@link IdentityServiceInMemoryUnderlyingStore} backs up all assigned OIDs in the injected
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
            boolean useDescriptorsBasedCache) {
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
        initializeIdentityCache(perProbeMetadata, useDescriptorsBasedCache);
    }

    @VisibleForTesting
    public IdentityServiceInMemoryUnderlyingStore(@Nonnull final IdentityDatabaseStore identityDatabaseStore,
                                                  final int initializationTimeoutMin,
                                                  @Nonnull final ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata,
                                                  boolean useDescriptorsBasedCache) {
        this.identityDatabaseStore = identityDatabaseStore;
        this.initializationTimeoutMin = initializationTimeoutMin;
        this.identityCache = useDescriptorsBasedCache ? new DescriptorsBasedCache() : new OptimizedIdentityRecordsBasedCache(perProbeMetadata);
        addRestoredIds(Collections.emptySet());
    }

    /**
     * Initialize the store.
     */
    @Override
    public void initialize() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        LOGGER.info("Begin loading the identity store");
        executor.execute(
            new SavedIdsLoader(identityDatabaseStore, loadIdsInterval, loadIdsTimeUnit,
                    this::addRestoredIds));
        executor.shutdown();
        try {
            boolean success = executor.awaitTermination(initializationTimeoutMin, TimeUnit.MINUTES);
            stopwatch.stop();
            if (!success) {
                LOGGER.error("Failed to load the identity store due to time out, elapsed={}", stopwatch);
            } else {
                LOGGER.info("Loaded the identity store, elapsed={}", stopwatch);
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
                    try {
                        Thread.sleep(retryMs);
                    } catch (InterruptedException e1) {
                        LOGGER.error("Interrupted while loading saved IDs from the DB.", e1);
                        Thread.currentThread().interrupt();
                        return;
                    }
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
                initializationLock.wait(initializationTimeoutMin * 60000L);
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
    static public @Nonnull String composeKeyFromProperties(
        @Nonnull List<PropertyDescriptor> properties) {
        StringBuilder sb = new StringBuilder();
        for (PropertyDescriptor vpd : properties) {
            sb.append(propertyAsString(vpd));
        }
        return sb.toString();
    }

    @Override
    public long lookupByIdentifyingSet(@Nonnull List<PropertyDescriptor> nonVolatileProperties,
            @Nonnull  List<PropertyDescriptor> volatileProperties)
            throws IdentityServiceStoreOperationException, IdentityUninitializedException {
        checkInitialized();
        return identityCache.getOidByIdentifyingProperties(nonVolatileProperties, volatileProperties);
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
        Stopwatch stopwatch = Stopwatch.createStarted();
        synchronized (initializationLock) {
            if (!initialized) {
                LoadReport loadReport = new LoadReport();
                identityRecordsToProbeId.forEach(identityRecord -> {
                    EntityInMemoryProxyDescriptor descriptor = identityRecord.getDescriptor();
                    EntityInMemoryProxyDescriptor existing =
                        identityCache.addIdentityRecord(identityRecord, loadReport);
                    if (existing != null && !existing.equals(descriptor)) {
                        // If we don't initialize the IdentityService until the restoration from the
                        // database completes, then this should never happen.
                        LOGGER.error("The ID {} was associated with a different set of properties"
                                + " than the ones retrieved from the database.", descriptor.getOID());
                    }
                });
                LOGGER.info("Identity Cache initialized in {} with {} records", stopwatch, identityRecordsToProbeId.size());
                LOGGER.info(loadReport.generate());
                if (LOGGER.isDebugEnabled()) {
                    identityCache.report();
                }
                setStoreAsInitializedAndNotify();
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeEntry(long oid)
        throws IdentityServiceStoreOperationException, IdentityUninitializedException {
        checkInitialized();

        final boolean successfulRemoval = identityCache.remove(Collections.singleton(oid)) == 1;
        if (successfulRemoval) {
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
     * Delete OIDs from the in memory cache but leave them in the database. This is used
     * for expiring OIDs (see {@link com.vmturbo.topology.processor.identity.StaleOidManager}).
     *
     * @param oidsToRemove oids to remove.
     * @return true if the oid was found in the cache; false if not.
     */
    @Override
    public int bulkRemove(Set<Long> oidsToRemove) {
        return identityCache.remove(oidsToRemove);
    }

    @Override
    public Set<Long> getCurrentOidsInIdentityCache() throws IdentityUninitializedException {
        synchronized (initializationLock) {
            if (!initialized) {
                throw new IdentityUninitializedException();
            }
        }
        return identityCache.getOids();
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
     * Creates a {@link IdentityRecordsOperation}.
     * @return the IdentityRecordsOperation
     * @throws IdentityUninitializedException if the store hasn't been initialized yet
     */
    public IdentityRecordsOperation createTransaction() throws IdentityUninitializedException {
        checkInitialized();
        return new IdentityRecordsOperation(identityCache, identityDatabaseStore);
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
    public void backup(@Nonnull final Writer writer) {
        identityCache.toJson(writer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restore(@Nonnull final Reader input, @Nonnull Map<Long,
        ServiceEntityIdentityMetadataStore> perProbeMetadata) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        synchronized (initializationLock) {
            // We don't check if initialized is true or not, because the "restore" overrides
            // whatever was initialized anyway.
            // But we set the initialized flag to true, so that no information gets lost if
            // "restore" happens before ID data is re-loaded.
            initialized = true;
            identityCache.clear();
            LoadReport loadReport = new LoadReport();
            List<IdentityRecord> identityRecords = constructGson()
                    .fromJson(input, new TypeToken<List<IdentityRecord>>() {
                    }.getType());
            identityRecords.forEach(identityRecord -> {
                identityCache.addIdentityRecord(identityRecord, loadReport);
            });
            LOGGER.info("Identity Cache initialized in {} with {} records", stopwatch.stop().elapsed(TimeUnit.SECONDS), identityRecords.size());
            LOGGER.info(loadReport.generate());
            if (LOGGER.isDebugEnabled()) {
                identityCache.report();
            }
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
     * Initialize the cache based on the config parameter useIdentityRecordsCache.
     * @param perProbeMetadata probes with their metadata
     * @param useDescriptorsBasedCache whether to use the {@link DescriptorsBasedCache} or
     * the {@link OptimizedIdentityRecordsBasedCache}
     *
     */
    private void initializeIdentityCache(ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata, boolean useDescriptorsBasedCache) {
        if (useDescriptorsBasedCache) {
            this.identityCache = new DescriptorsBasedCache();
        } else {
            this.identityCache = new OptimizedIdentityRecordsBasedCache(perProbeMetadata);
        }
        LOGGER.info("Chosen the following Identity Cache: {}", this.identityCache.getClass().getSimpleName());
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

    /**
     * Class that adds records to the cache, and then, stores them in the database. We do this in
     * two phases for two reasons: 1) we need to update the cache as soon as we assign a new oid, so that if
     * a duplicate entity comes into the same response, we return the same oid. 2) We do not want to
     * perform one write per new oid, but instead, batch them and do one for all the new oids in
     * a discovery response.
     */
    public static class IdentityRecordsOperation implements AutoCloseable {
        /**
         * Track the time taken to store a batch of oids.IdentityRecordsBasedCache.
         */
        private static final DataMetricSummary OID_STORING_TIME = DataMetricSummary.builder()
                .withName("tp_oid_storing_seconds")
                .withHelp("Time (in seconds) spent storing entity oids.")
                .build()
                .register();

        private final List<IdentityRecord> identityRecordsToUpdate = new ArrayList<>();
        private final List<IdentityRecord> oldIdentityRecords = new ArrayList<>();
        private final IdentityCache identityCache;
        private final IdentityDatabaseStore identityDatabaseStore;
        private final Set<Long> addedOids = new HashSet<>();

        /**
         * Created an IdentityRecordsOperation.
         * @param identityCache the cache with the records
         * @param identityDatabaseStore the underlying database
         */
        public IdentityRecordsOperation(final IdentityCache identityCache, IdentityDatabaseStore identityDatabaseStore) {
            this.identityCache = identityCache;
            this.identityDatabaseStore = identityDatabaseStore;
        }

        /**
         * Add an entry to the transaction. This will add the entry to the cache.
         * @param oid of the entity
         * @param entryData the entity to add
         * @throws IdentityUninitializedException if the identity store was not initialized
         */
        public void addEntry(final long oid, final EntryData entryData)
                throws IdentityUninitializedException {
            final EntityInMemoryProxyDescriptor vmtPD;
            vmtPD = new EntityInMemoryProxyDescriptor(oid,
                    entryData.getDescriptor(),
                    entryData.getMetadata());
            final EntityType entityType = entryData.getEntityType();
            final IdentityRecord identityRecord = new IdentityRecord(entityType, vmtPD, entryData.getProbeId());

            //If the oid is present and the entity hasn't changed, do nothing
            final EntityInMemoryProxyDescriptor existingDescriptor = identityCache.get(oid);
            if (Objects.equals(existingDescriptor, vmtPD)) {
                return;
            }
            if (existingDescriptor == null) {
                addedOids.add(oid);
            } else {
                // Cache the existing records before updating them, in case we need to roll back the changes
                oldIdentityRecords.add(new IdentityRecord(entityType, existingDescriptor, entryData.getProbeId()));
            }

            identityCache.addIdentityRecord(identityRecord);
            identityRecordsToUpdate.add(identityRecord);
        }

        /**
         * This method saves all the newly added record to the cache, as part of this transaction,
         * to the database. It will always be invoked once a transaction gets closed.
         * @throws IdentityServiceStoreOperationException if there's an issue with the database
         */
        @Override
        public void close() throws IdentityServiceStoreOperationException {
            if (identityRecordsToUpdate.size() > 0) {
                Stopwatch stopwatch = Stopwatch.createStarted();
                try (DataMetricTimer timer = OID_STORING_TIME.startTimer()) {
                    identityDatabaseStore.saveDescriptors(identityRecordsToUpdate);
                } catch (IdentityDatabaseException e) {
                    final String error = String.format("Could not store descriptors for %d entities",
                            addedOids.size());
                    LOGGER.error(error);
                    identityCache.remove(addedOids);
                    // Revert the non updated identity records
                    oldIdentityRecords.forEach(identityCache::addIdentityRecord);
                    throw new IdentityServiceStoreOperationException(error, e);
                }
                LOGGER.info("Successfully stored entity ID assignment descriptors, "
                                + "count={}, duration={}",
                        identityRecordsToUpdate.size(), stopwatch);
            }
        }
    }
}
