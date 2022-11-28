package com.vmturbo.topology.processor.identity;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityView;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.ITransport;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.identity.exceptions.IdentityServiceException;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityIdentifyingPropertyValues;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.identity.cache.DescriptorsBasedCache;
import com.vmturbo.topology.processor.identity.cache.OptimizedIdentityRecordsBasedCache;
import com.vmturbo.topology.processor.identity.extractor.EntityDescriptorImpl;
import com.vmturbo.topology.processor.identity.extractor.IdentifyingPropertyExtractor;
import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadata;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadataStore;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityProperty;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;
import com.vmturbo.topology.processor.probes.ProbeInfoCompatibilityChecker;

/**
 * Base implementation of the Identity Provider.
 *
 * <p>This implementation relies on the Identity Service
 * for entity OID assignment using probe-provided
 * metadata.
 *
 * <p>This implementation handles OID assignment for targets,
 * but the intention (as of May, 2016) is to eventually
 * expand the Identity Service to handle target OID assignment
 * based on probe-provided metadata as well.
 *
 * <p>This implementation handles OID assignment of other
 * objects (probes, discoveries, targets, etc.) as well, and
 * there is no plan to outsource that to the Identity Service
 * since those are only relevant for intra-Topology-Processor
 * functionality.
 */
@ThreadSafe
public class IdentityProviderImpl implements IdentityProvider {

    // ---------------------------
    // START diags-related constants

    /**
     * File name inside diagnostics to store identity information.
     */
    public static final String ID_DIAGS_FILE_NAME = "Identity";
    /**
     * The total number of diags entries. There should be this many DIAGS_*_IDX variables.
     */
    private static final int NUM_DIAGS_ENTRIES = 3;

    private static final int DIAGS_PROBE_TYPE_IDX = 0;

    private static final int DIAGS_PROBE_METADATA_IDX = 1;

    private static final int DIAGS_ID_SVC_IDX = 2;
    // END diags-related constants
    // ---------------------------

    private static final String PROBE_ID_PREFIX = "id/probes/";

    private final Logger logger = LogManager.getLogger();

    // START Fields for Probe ID management
    private final ConcurrentMap<String, Long> probeTypeToId;

    private final Object probeIdLock = new Object();
    // END Fields for Probe ID management

    // START Fields for Entity ID management
    /**
     * Single-entry lock because the Identity Service is not thread-safe.
     */
    private final Object identityServiceLock = new Object();

    private final IdentityService identityService;

    private final KeyValueStore keyValueStore;

    private final ProbeInfoCompatibilityChecker probeInfoCompatibilityChecker;

    private final IdentityServiceInMemoryUnderlyingStore identityServiceInMemoryUnderlyingStore;

    /**
     * Contains the entity identity metadata each probe provided at registration
     * time. We use this metadata to drive property extraction from entity DTOs.
     */
    private final ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata
            = new ConcurrentHashMap<>();

    /**
     * Contains the {@link ProbeInfo} for each probe, provided at registration time.
     * We use this to enforce compatibility when a probe of the same type re-registers with
     * the topology processor.
     */
    private final ConcurrentMap<Long, ProbeInfo> existingProbeInfoById = new ConcurrentHashMap<>();
    // END Fields for Entity ID management

    private final StaleOidManager staleOidManager;

    /**
     * Create a new IdentityProvider implementation.
     * @param keyValueStore The key value store where identity information that needs to be persisted is stored
     * @param compatibilityChecker compatibility checker
     * @param identityGeneratorPrefix The prefix used to initialize the {@link IdentityGenerator}
     * @param identityDatabaseStore the store containing the oids
     * @param identityStoreinitializationTimeoutMin the maximum time that threads will wait for
* the store to be ready
     * @param assignedIdReloadReattemptIntervalSeconds The interval at which to attempt to reload assigned IDs
* {@link DescriptorsBasedCache}
     * @param staleOidManager used to expire stale oids
     * @param useDescriptorsBasedCache whether to use a {@link DescriptorsBasedCache} or a {@link OptimizedIdentityRecordsBasedCache}
     */
    public IdentityProviderImpl(@Nonnull final KeyValueStore keyValueStore,
                                @Nonnull final ProbeInfoCompatibilityChecker compatibilityChecker,
                                final long identityGeneratorPrefix,
                                @Nonnull IdentityDatabaseStore identityDatabaseStore,
                                int identityStoreinitializationTimeoutMin,
                                long assignedIdReloadReattemptIntervalSeconds,
                                @Nonnull final StaleOidManager staleOidManager,
                                final boolean useDescriptorsBasedCache) {
        IdentityGenerator.initPrefix(identityGeneratorPrefix);
        this.identityServiceInMemoryUnderlyingStore =
            new IdentityServiceInMemoryUnderlyingStore(identityDatabaseStore, identityStoreinitializationTimeoutMin,
            assignedIdReloadReattemptIntervalSeconds, TimeUnit.SECONDS, perProbeMetadata, useDescriptorsBasedCache);
        this.identityService = new IdentityService(identityServiceInMemoryUnderlyingStore,
            new HeuristicsMatcher());
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        this.probeInfoCompatibilityChecker = Objects.requireNonNull(compatibilityChecker);
        this.staleOidManager = staleOidManager;
        Map<String, String> savedProbeIds = this.keyValueStore.getByPrefix(PROBE_ID_PREFIX);
        this.probeTypeToId = savedProbeIds.entrySet().stream().collect(Collectors.toConcurrentMap(
            entry -> entry.getKey().replaceFirst(PROBE_ID_PREFIX, ""),
            entry -> Long.parseLong(entry.getValue())));
      }

    /**
     * Create a new IdentityProvider implementation for testing. With this implementation the
     * identityService can be mocked and stubbed and make it easier to test interactions with it.
     *
     * @param identityService The identity service to use when identifying service entities
     * @param keyValueStore The key value store where identity information that needs to be persisted is stored
     * @param identityGeneratorPrefix The prefix used to initialize the {@link IdentityGenerator}
     * @param compatibilityChecker compatibility checker
     * @param staleOidManager used to expire stale oids
     */
    @VisibleForTesting
    public IdentityProviderImpl(@Nonnull final IdentityService identityService,
                                @Nonnull final KeyValueStore keyValueStore,
                                @Nonnull final ProbeInfoCompatibilityChecker compatibilityChecker,
                                final long identityGeneratorPrefix,
                                @Nonnull final StaleOidManager staleOidManager) {
        IdentityGenerator.initPrefix(identityGeneratorPrefix);
        this.identityService = identityService;
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        this.probeInfoCompatibilityChecker = Objects.requireNonNull(compatibilityChecker);
        this.identityServiceInMemoryUnderlyingStore = null;
        this.staleOidManager = staleOidManager;
        Map<String, String> savedProbeIds = this.keyValueStore.getByPrefix(PROBE_ID_PREFIX);
        this.probeTypeToId = savedProbeIds.entrySet().stream().collect(Collectors.toConcurrentMap(
            entry -> entry.getKey().replaceFirst(PROBE_ID_PREFIX, ""),
            entry -> Long.parseLong(entry.getValue())));
    }

    /** {@inheritDoc}
     */
    @Override
    public long getTargetId(@Nonnull TargetSpec targetSpec) {
        /*
         * TODO (roman.zimine @ May 26, 2015):
         * we should provide a means to provide the same target ID
         * when two target specs meet some probe-provided standard
         * of equivalence.
         */
        Objects.requireNonNull(targetSpec);
        return IdentityGenerator.next();
    }

    @Override
    public long getProbeRegistrationId(@Nonnull ProbeInfo probeInfo,
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport) {
        Objects.requireNonNull(probeInfo);
        Objects.requireNonNull(transport);
        return IdentityGenerator.next();
    }

    /** {@inheritDoc}
     */
    @Override
    public long getProbeId(@Nonnull final ProbeInfo probeInfo) throws IdentityProviderException {
        Objects.requireNonNull(probeInfo);
        // The probe type uniquely identifies a probe.
        synchronized (probeIdLock) {
            final String probeType = probeInfo.getProbeType();
            Long probeId = probeTypeToId.get(probeType);
            if (probeId == null) {
                probeId = IdentityGenerator.next();
                // Store the probe ID in Consul
                storeProbeId(probeType, probeId);
                // Cache the probe ID in memory
                probeTypeToId.put(probeType, probeId);
            }

            final ProbeInfo existingInfo = existingProbeInfoById.putIfAbsent(probeId, probeInfo);
            if (existingInfo != null) {
                final boolean compatible =
                    probeInfoCompatibilityChecker.areCompatible(existingInfo, probeInfo);
                if (!compatible) {
                    throw new IdentityProviderException("Probe configuration " + probeInfo
                        + " is incompatible with already registered probe with the same probe type: "
                        + existingInfo);
                }
                // Now that we passed the compatibility check we can override the probe info
                // with the most recent one.
                existingProbeInfoById.put(probeId, probeInfo);
            }

            // We passed the compatibility check, so replace the per-probe metadata with the
            // most recent version.
            perProbeMetadata.put(probeId,
                new ServiceEntityIdentityMetadataStore(probeInfo.getEntityMetadataList()));

            return probeId;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateProbeInfo(ProbeInfo probeInfo) {
        synchronized (probeIdLock) {
            Long probeId = probeTypeToId.get(probeInfo.getProbeType());
            if (probeId == null) {
                logger.warn("Trying to update a non-existent probeInfo: {}", probeInfo);
                return;
            }
            if (!perProbeMetadata.containsKey(probeId)) {
                logger.warn("ProbeInfo doesn't exist in the ProbeMetadataMap: {}", probeInfo);
                return;
            }
            existingProbeInfoById.put(probeId, probeInfo);
            perProbeMetadata.put(probeId,
                    new ServiceEntityIdentityMetadataStore(probeInfo.getEntityMetadataList()));
        }
    }

    @Override
    public Set<Long> getCurrentOidsInIdentityCache() throws IdentityUninitializedException {
        return identityServiceInMemoryUnderlyingStore.getCurrentOidsInIdentityCache();
    }

    @Override
    public IdentityServiceUnderlyingStore getStore() {
        return identityServiceInMemoryUnderlyingStore;
    }

    @Override
    public void initializeStaleOidManager(@Nonnull final Supplier<Set<Long>> getCurrentOids) {
        this.staleOidManager.initialize(getCurrentOids, this::removeStaleOidsFromCache);
    }

    @Override
    public int expireOids() throws InterruptedException, ExecutionException, TimeoutException {
        return this.staleOidManager.expireOidsImmediatly();
    }

    /** {@inheritDoc}
     */
    @Override
    public Map<Long, EntityDTO> getIdsForEntities(final long probeId,
            @Nonnull final List<EntityDTO> entityDTOs) throws IdentityServiceException {
        final List<EntityDTO> immutableEntityDTOs = ImmutableList.copyOf(Objects.requireNonNull(entityDTOs));
        /* We expect that the probe is already registered.
         * There is a small window in getProbeId() where concurrent calls could
         * return an ID without an associated entry in perProbeMetadata.
         * However, since we don't expect entity ID processing to happen until
         * after the probe is fully registered this should be a non-issue, and
         * it's safe to expect the probe to be registered.
         */
        final ServiceEntityIdentityMetadataStore probeMetadata =
                Objects.requireNonNull(perProbeMetadata.get(probeId));
        final List<EntryData> entryData = new ArrayList<>(entityDTOs.size());
        for (EntityDTO dto : immutableEntityDTOs) {
            // Find the identity metadata the probe provided for this
            // entity type.
            ServiceEntityIdentityMetadata entityMetadata =
                probeMetadata.getMetadata(dto.getEntityType());
            // There may not be identity metadata if the probe didn't
            // provide any. That means every time we discover that entity
            // we'll assign it a different OID.
            if (entityMetadata != null) {
                final EntityDescriptor descriptor =
                    IdentifyingPropertyExtractor.extractEntityDescriptor(dto, entityMetadata);
                entryData.add(new EntryData(descriptor, entityMetadata, probeId, dto.getEntityType()));
            } else {
                // If we are unable to assign an OID for an entity, abandon the attempt.
                // One missing entity OID spoils the entire batch because of how tangled the relationships
                // between entities are.
                throw new IdentityServiceException(
                        "Probe " + probeId + " sends entities of type " + dto.getEntityType()
                                + " but provides no related " + "identity metadata.");
            }
        }
        return getOidsForObjects(entryData, immutableEntityDTOs);
    }

    @Override
    public long getCloneId(@Nonnull final TopologyEntityView inputEntity) {
        return IdentityGenerator.next();
    }

    /** {@inheritDoc}
     */
    @Override
    public long generateOperationId() {
        return IdentityGenerator.next();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long generateTopologyId() {
        return IdentityGenerator.next();
    }

    /**
     * Backward compatible implementation of collect method which appends String diags.
     *
     * @param appender appender for the Identity properties.
     * @throws DiagnosticsException if unable to append the identity properties.
     */
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        logger.info("Collecting diagnostics from the Identity Provider...");
        // No-pretty-print is important, because we want one line per item so that we
        // can restore properly.
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        try {
            // Synchronize on the probeIdLock so that probes that register
            // during a diags dump don't cause any issues or inconsistencies.
            synchronized (probeIdLock) {
                appender.appendString(gson.toJson(probeTypeToId));
                appender.appendString(gson.toJson(perProbeMetadata));
                final StringWriter writer = new StringWriter();
                identityService.backup(writer);
                appender.appendString(writer.toString());
            }
        } finally {
            logger.info("Finished collecting diagnostics from the Identity Provider.");
        }
    }

    @Override
    public void collectDiags(@NotNull OutputStream appender)
            throws DiagnosticsException, IOException {
        logger.info("Collecting diagnostics from the Identity Provider...");
        // No-pretty-print is important, because we want one line per item so that we
        // can restore properly.
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();

        try (Writer writer = new OutputStreamWriter(CloseShieldOutputStream.wrap(appender),
                    StandardCharsets.UTF_8)) {
            // Synchronize on the probeIdLock so that probes that register
            // during a diags dump don't cause any issues or inconsistencies.
            synchronized (probeIdLock) {
                gson.toJson(probeTypeToId, writer);
                writer.append("\n");
                gson.toJson(perProbeMetadata, writer);
                writer.append("\n");
                identityService.backup(writer);
                writer.append("\n");
            }
        } catch (Exception e) {
            logger.error("Failed to collect diagnostics from the Identity Provider: ", e);
        }
        logger.info("Finished collecting diagnostics from the Identity Provider.");
    }

    @Override
    public void restoreDiags(@NotNull byte[] bytes, @Nullable Void context)
            throws DiagnosticsException {
        logger.info("Restoring diagnostics to the Identity Provider...");
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        synchronized (probeIdLock) {
            try {
                InputStream inputStream = new ByteArrayInputStream(bytes);
                BufferedReader bfReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                String diagLine = null;
                int lineCounter = -1;
                while ((diagLine = bfReader.readLine()) != null) {
                    lineCounter++;
                    switch (lineCounter) {
                        case(DIAGS_PROBE_TYPE_IDX):
                            restoreDiagsProbeType(gson, diagLine);
                            break;
                        case(DIAGS_PROBE_METADATA_IDX):
                            restoreDiagsProbeMetadata(gson, diagLine);
                            break;
                        case(DIAGS_ID_SVC_IDX):
                            restoreDiagsIdSvc(diagLine);
                            break;
                        default:
                            logger.error("Line {} not mapped to Identity Diags property.", lineCounter);
                            break;
                    }
                }
            } catch (IOException e) {
                logger.error("Failed to restore diagnostics to the Identity Provider: ", e);
            }
        }
        logger.info("Successfully restored the Identity Provider!");
    }

    @Override
    public void restoreStringDiags(@Nonnull final List<String> diagsLines, @Nullable Void context) {
        logger.info("Restoring diagnostics to the Identity Provider...");
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        synchronized (probeIdLock) {
            // The restore has to be in the same order as the collection.
            // There is no backwards-compatibility for now.
            if (diagsLines.size() != NUM_DIAGS_ENTRIES) {
                throw new IllegalArgumentException("Unexpected size of diags to restore from.");
            }

            restoreDiagsProbeType(gson, diagsLines.get(DIAGS_PROBE_TYPE_IDX));
            restoreDiagsProbeMetadata(gson, diagsLines.get(DIAGS_PROBE_METADATA_IDX));
            restoreDiagsIdSvc(diagsLines.get(DIAGS_ID_SVC_IDX));
        }
        logger.info("Successfully restored the Identity Provider!");
    }

    private void restoreDiagsProbeType(Gson gson, String diagLine) {
        try {
            final Map<String, Long> newProbeTypeToId = gson.fromJson(diagLine,
                    new TypeToken<Map<String, Long>>(){}.getType());
            probeTypeToId.clear();
            probeTypeToId.putAll(newProbeTypeToId);
            // Keep Consul in sync with the internal cache
            keyValueStore.removeKeysWithPrefix(PROBE_ID_PREFIX);
            probeTypeToId.forEach(this::storeProbeId);
        } catch (JsonSyntaxException e) {
            throw new IllegalArgumentException(
                    "Unable to parse probe type to ID input JSON.", e);
        }
    }

    private void restoreDiagsProbeMetadata(Gson gson, String diagLine) {
        try {
            final Map<Long, ServiceEntityIdentityMetadataStore> newPerProbe = gson.fromJson(diagLine,
                    new TypeToken<Map<Long, ServiceEntityIdentityMetadataStore>>(){}.getType());
            perProbeMetadata.clear();
            perProbeMetadata.putAll(newPerProbe);
        } catch (JsonSyntaxException e) {
            throw new IllegalArgumentException(
                    "Unable to parse probe metadata input JSON.", e);
        }
    }

    private void restoreDiagsIdSvc(String diagLine) {
        try {
            final StringReader reader = new StringReader(diagLine);
            identityService.restore(reader, perProbeMetadata);
        } catch (Exception e) {
            final StringReader reader = new StringReader(diagLine);
            identityService.restoreOldDiags(reader);
        }
    }

    /**
     * Makes the current thread wait until the store gets initialized or a timeout occurs.
     * @throws InterruptedException if any thread interrupted the current thread before
     * or while the current thread was waiting for a notification.
     */
    public void waitForInitializedStore() throws InterruptedException {
         identityService.waitForInitializedStore();
    }

    @Nonnull
    @Override
    public String getFileName() {
        return ID_DIAGS_FILE_NAME;
    }

    private void storeProbeId(final String probeType, final Long probeId) {
        keyValueStore.put(PROBE_ID_PREFIX + probeType, probeId.toString());
    }

    @Override
    public void initialize() throws InitializationException {
        identityServiceInMemoryUnderlyingStore.initialize();
    }

    private void removeStaleOidsFromCache(@Nonnull Set<Long> staleOids) {
        synchronized (identityServiceLock) {
            final long startTime = System.currentTimeMillis();
            final long removedOids = identityServiceInMemoryUnderlyingStore.bulkRemove(staleOids);
            if (staleOids.size() > 0) {
                logger.info("Removed {} oids from in memory cache in {} milliseconds. "
                                + "{} oids were not found in cache.", removedOids,
                        System.currentTimeMillis() - startTime, staleOids.size() - removedOids);
            }
        }
    }

    @Nonnull
    @Override
    public Map<Long, EntityIdentifyingPropertyValues> getIdsFromIdentifyingPropertiesValues(
        final long probeId, @Nonnull final List<EntityIdentifyingPropertyValues> entityIdentifyingPropertyValues)
        throws IdentityServiceException {
        final List<EntityIdentifyingPropertyValues> immutableEntityIdentifyingPropertyValues =
            ImmutableList.copyOf(Objects.requireNonNull(entityIdentifyingPropertyValues));
        final List<EntryData> entryData = new ArrayList<>(immutableEntityIdentifyingPropertyValues.size());
        final ServiceEntityIdentityMetadataStore probeMetadata = perProbeMetadata.get(probeId);
        for (final EntityIdentifyingPropertyValues identifyingPropValues : immutableEntityIdentifyingPropertyValues) {
            entryData.add(createEntryDataForEntityIdentifyingPropertyValues(identifyingPropValues, probeMetadata,
                probeId));
        }
        return getOidsForObjects(entryData, immutableEntityIdentifyingPropertyValues);
    }

    private EntryData createEntryDataForEntityIdentifyingPropertyValues(
        final EntityIdentifyingPropertyValues identifyingPropertyValues,
        final ServiceEntityIdentityMetadataStore probeMetadata, final long probeId) throws IdentityServiceException {

        final EntityDTO.EntityType entityType = getEntityTypeFromEntityIdentifyingPropertyValues(
            identifyingPropertyValues);
        final Map<String, String> identifyingProperties =
            buildIdentifyingPropertiesMapFromEntityIdentifyingPropertyValues(identifyingPropertyValues, entityType);

        final ServiceEntityIdentityMetadata entityMetadata = getEntityIdentityMetadataForEntityType(probeMetadata,
            entityType, probeId);
        final List<ServiceEntityProperty> entityVolatileProps = entityMetadata.getVolatileProperties();
        final List<ServiceEntityProperty> entityNonVolatileProps = entityMetadata.getNonVolatileProperties();
        final List<ServiceEntityProperty> entityHeuristicsProps = entityMetadata.getHeuristicProperties();

        final EntityDescriptor descriptor = buildEntityDescriptor(identifyingProperties,
            entityVolatileProps, entityNonVolatileProps, entityHeuristicsProps);
        final EntityMetadataDescriptor metadataDescriptor = new ServiceEntityIdentityMetadata(
            entityNonVolatileProps, entityVolatileProps, entityHeuristicsProps,
            entityMetadata.getHeuristicThreshold());
        return new EntryData(descriptor, metadataDescriptor, probeId, entityType);
    }

    private EntityDTO.EntityType getEntityTypeFromEntityIdentifyingPropertyValues(
        final EntityIdentifyingPropertyValues identifyingPropertyValues) throws IdentityServiceException {
        if (!identifyingPropertyValues.hasEntityType()) {
            throw new IdentityServiceException("Entity type not set for EntityIdentifyingPropertyValues: "
                + identifyingPropertyValues);
        }
        return identifyingPropertyValues.getEntityType();
    }

    private Map<String, String> buildIdentifyingPropertiesMapFromEntityIdentifyingPropertyValues(
        final EntityIdentifyingPropertyValues identifyingPropertyValues, final EntityDTO.EntityType entityType)
        throws IdentityServiceException {
        final Map<String, String> identifyingProperties = new HashMap<>(
            identifyingPropertyValues.getIdentifyingPropertyValuesMap());
        final String entityTypeFieldName = entityType.getValueDescriptor().getType().getName();
        if (!identifyingProperties.containsKey(entityTypeFieldName)) {
            identifyingProperties.put(entityTypeFieldName, entityType.name());
        } else {
            if (!entityType.name().equals(identifyingProperties.get(entityTypeFieldName))) {
                throw new IdentityServiceException(
                    "Entity type value in the identifyingProperties does not match the type of entityType param.");
            }
        }
        return identifyingProperties;
    }

    private ServiceEntityIdentityMetadata getEntityIdentityMetadataForEntityType(
        final ServiceEntityIdentityMetadataStore probeMetadata, final EntityDTO.EntityType entityType,
        final long probeId) throws IdentityServiceException {
        final ServiceEntityIdentityMetadata entityMetadata = probeMetadata.getMetadata(entityType);
        if (entityMetadata == null) {
            throw new IdentityServiceException(
                String.format("No Identity metadata registered for Entity type %s, probe id %s",
                    entityType, probeId));
        }
        return entityMetadata;
    }

    private EntityDescriptor buildEntityDescriptor(final Map<String, String> identifyingProperties,
                                                   final List<ServiceEntityProperty> entityVolatileProps,
                                                   final List<ServiceEntityProperty> entityNonVolatileProps,
                                                   final List<ServiceEntityProperty> entityHeuristicsProps)
        throws IdentityServiceException {
        final List<PropertyDescriptor> volatileProperties = extractProperties(identifyingProperties,
            entityVolatileProps);
        final List<PropertyDescriptor> nonVolatileProperties = extractProperties(identifyingProperties,
            entityNonVolatileProps);
        final List<PropertyDescriptor> heuristicsProperties = extractProperties(identifyingProperties,
            entityHeuristicsProps);
        return new EntityDescriptorImpl(nonVolatileProperties, volatileProperties, heuristicsProperties);
    }

    private <T> Map<Long, T> getOidsForObjects(final List<EntryData> entryData, final List<T> objects)
        throws IdentityServiceException {
        final List<Long> ids;
        synchronized (identityServiceLock) {
            ids = identityService.getOidsForObjects(entryData);
        }
        final Map<Long, T> result = new HashMap<>();
        for (int i = 0; i < ids.size(); i++) {
            result.put(ids.get(i), objects.get(i));
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Long> getOidFromProperties(Map<String, String> identifyingProperties, long probeId,
            EntityDTO.EntityType entityType)
            throws IdentityServiceException, IdentityServiceStoreOperationException,
            IdentityUninitializedException {

        // get probe metadata
        final ServiceEntityIdentityMetadataStore probeMetadata =
                perProbeMetadata.get(probeId);

        // entity type has to be part of non-volatile properties, hence adding it in identifyingProperties
        String entityTypeFieldName = entityType.getValueDescriptor().getType().getName();
        if (!identifyingProperties.containsKey(entityTypeFieldName)) {
            identifyingProperties.put(entityTypeFieldName, entityType.name());
        } else {
            if (!entityType.name().equals(identifyingProperties.get(entityTypeFieldName))) {
                throw new IdentityServiceException(
                        "Entity type value in the identifyingProperties does not match the type of entityType param.");
            }
        }

        ServiceEntityIdentityMetadata entityMetadata = probeMetadata.getMetadata(entityType);
        List<ServiceEntityProperty> entityVolatileProps = entityMetadata.getVolatileProperties();
        List<PropertyDescriptor> volatileProperties = extractProperties(identifyingProperties,
                entityVolatileProps);
        List<ServiceEntityProperty> entityNonVolatileProps = entityMetadata.getNonVolatileProperties();
        List<PropertyDescriptor> nonVolatileProperties = extractProperties(identifyingProperties,
                entityNonVolatileProps);
        List<ServiceEntityProperty> entityHeuristicsProps = entityMetadata.getHeuristicProperties();
        List<PropertyDescriptor> heuristicsProperties = extractProperties(identifyingProperties,
                entityHeuristicsProps);

        EntityDescriptor descriptor = new EntityDescriptorImpl(nonVolatileProperties, volatileProperties,
                heuristicsProperties);
        EntityMetadataDescriptor metadataDescriptor = new ServiceEntityIdentityMetadata(entityNonVolatileProps,
                entityVolatileProps, entityHeuristicsProps, entityMetadata.getHeuristicThreshold());

        long existingOid = identityService.getOidFromProperties(nonVolatileProperties, volatileProperties,
                heuristicsProperties, descriptor, metadataDescriptor);

        if (existingOid == identityService.INVALID_OID) {
            return Optional.empty();
        }
        return Optional.of(existingOid);
    }

    /**
     * Utility to generate a List of property descriptors from a map of strings.
     *
     * @param inputProperties   Collection that contains property name as key and a string as value.
     *                          Ie: <code>{id : 123, displayName: vm1, tag: myTag}</code>.
     * @param entityProperties  List of {@link ServiceEntityProperty} to pull from input Properties.
     * @return  List of {@link PropertyDescriptor}.
     * @throws IdentityServiceException  if unable to fetch the entity property.
     */
    private List<PropertyDescriptor> extractProperties(Map<String, String> inputProperties,
            List<ServiceEntityProperty> entityProperties) throws IdentityServiceException {
        List<PropertyDescriptor> extractedProperties = new ArrayList<>(entityProperties.size());
        for (ServiceEntityProperty prop : entityProperties) {
            String propValue = inputProperties.get(prop.name);
            if (propValue != null) {
                extractedProperties.add(new PropertyDescriptorImpl(propValue, prop.groupId));
            } else {
                // If we are unable to find a property abandon the attempt.
                throw new IdentityServiceException(
                        "Property " + prop.name + " is not present in the input identifying properties.");
            }
        }
        return extractedProperties;
    }
}
