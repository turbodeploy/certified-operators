package com.vmturbo.topology.processor.identity;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.identity.extractor.IdentifyingPropertyExtractor;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadata;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadataStore;

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

    /**
     * Contains the entity identity metadata each probe provided at registration
     * time. We use this metadata to drive property extraction from entity DTOs.
     */
    private final ConcurrentMap<Long, ServiceEntityIdentityMetadataStore> perProbeMetadata
            = new ConcurrentHashMap<>();
    // END Fields for Entity ID management

    /**
     * Create a new IdentityProvider implementation.
     *
     * @param identityService The identity service to use when identifying service entities
     * @param keyValueStore The key value store where identity information that needs to be persisted is stored
     * @param identityGeneratorPrefix The prefix used to initialize the {@link IdentityGenerator}
     */
    public IdentityProviderImpl(@Nonnull final IdentityService identityService,
                                @Nonnull final KeyValueStore keyValueStore,
                                final long identityGeneratorPrefix) {
        IdentityGenerator.initPrefix(identityGeneratorPrefix);
        this.identityService = Objects.requireNonNull(identityService);
        this.keyValueStore = Objects.requireNonNull(keyValueStore);

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

    /** {@inheritDoc}
     */
    @Override
    public long getProbeId(@Nonnull final ProbeInfo probeInfo) {
        Objects.requireNonNull(probeInfo);
        // The probe type uniquely identifies a probe.
        synchronized (probeIdLock) {
            Long probeId = probeTypeToId.get(probeInfo.getProbeType());
            if (probeId == null) {
                probeId = IdentityGenerator.next();

                keyValueStore.put(PROBE_ID_PREFIX + probeInfo.getProbeType(), probeId.toString());
                probeTypeToId.put(probeInfo.getProbeType(), probeId);
            }

            // We may have restored the probe ID from the KV store, in which case
            // it's still necessary to re-record the metadata the first time an
            // actual probe registers.
            if (!perProbeMetadata.containsKey(probeId)) {
                /* Instead of having a separate call-back for probes to
                 * register identity metadata we extract the metadata
                 * from the first probe that is assigned a particular ID.
                 *
                 * The assumption is that every subsequent probe that
                 * maps to the same ID should have the same identity
                 * metadata.
                 */
                perProbeMetadata.put(probeId,
                        new ServiceEntityIdentityMetadataStore(probeInfo.getEntityMetadataList()));
            }
            return probeId;
        }
    }

    /** {@inheritDoc}
     */
    @Override
    public Map<Long, EntityDTO> getIdsForEntities(final long probeId,
                                                  @Nonnull final List<EntityDTO> entityDTOs)
            throws IdentityUninitializedException, IdentityMetadataMissingException, IdentityProviderException {
        Objects.requireNonNull(entityDTOs);
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
        for (EntityDTO dto : entityDTOs) {
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
                entryData.add(new EntryData(descriptor, entityMetadata, probeId, dto));
            } else {
                // If we are unable to assign an OID for an entity, abandon the attempt.
                // One missing entity OID spoils the entire batch because of how tangled the relationships
                // between entities are.
                throw new IdentityMetadataMissingException(probeId, dto.getEntityType());
            }
        }

        final List<Long> ids;
        synchronized (identityServiceLock) {
            try {
                ids = identityService.getEntityOIDs(entryData);
            } catch (IdentityWrongSetException | IdentityServiceOperationException e) {
                throw new IdentityProviderException("Failed to assign IDs to entities.", e);
            }
        }

        final Map<Long, EntityDTO> retMap = new HashMap<>();
        for (int i = 0; i < ids.size(); ++i) {
            // All entry data objects will have entityDTOs, since we
            // put them there when constructing the entryData list.
            //
            // The sizes of entryData and ids should be the same, due to
            // the contract of identityService.getEntityOIDs.
            retMap.put(ids.get(i), entryData.get(i).getEntityDTO().get());
        }
        return retMap;
    }

    @Override
    public long getCloneId(@Nonnull final TopologyEntityDTOOrBuilder inputEntity) {
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

    @Nonnull
    @Override
    public List<String> collectDiags() {
        logger.info("Collecting diagnostics from the Identity Provider...");
        // No-pretty-print is important, because we want one line per item so that we
        // can restore properly.
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        // Synchronize on the probeIdLock so that probes that register
        // during a diags dump don't cause any issues or inconsistencies.
        final List<String> retList;
        synchronized (probeIdLock) {
            final StringWriter writer = new StringWriter();
            identityService.backup(writer);

            retList = new ArrayList<>(NUM_DIAGS_ENTRIES);
            retList.add(DIAGS_PROBE_TYPE_IDX, gson.toJson(probeTypeToId));
            retList.add(DIAGS_PROBE_METADATA_IDX, gson.toJson(perProbeMetadata));
            retList.add(DIAGS_ID_SVC_IDX, writer.toString());
        }
        logger.info("Finished collecting diagnostics from the Identity Provider.");
        return retList;
    }

    @Override
    public void restoreDiags(@Nonnull final List<String> diagsLines) {
        logger.info("Restoring diagnostics to the Identity Provider...");
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        synchronized (probeIdLock) {
            // The restore has to be in the same order as the collection.
            // There is no backwards-compatibility for now.
            if (diagsLines.size() != NUM_DIAGS_ENTRIES) {
                throw new IllegalArgumentException("Unexpected size of diags to restore from.");
            }

            try {
                final Map<String, Long> newProbeTypeToId = gson.fromJson(
                    diagsLines.get(DIAGS_PROBE_TYPE_IDX),
                    new TypeToken<Map<String, Long>>(){}.getType());
                probeTypeToId.clear();
                probeTypeToId.putAll(newProbeTypeToId);
            } catch (JsonSyntaxException e) {
                throw new IllegalArgumentException(
                        "Unable to parse probe type to ID input JSON.", e);
            }

            try {
                final Map<Long, ServiceEntityIdentityMetadataStore> newPerProbe = gson.fromJson(
                    diagsLines.get(DIAGS_PROBE_METADATA_IDX),
                    new TypeToken<Map<Long, ServiceEntityIdentityMetadataStore>>(){}.getType());
                perProbeMetadata.clear();
                perProbeMetadata.putAll(newPerProbe);
            } catch (JsonSyntaxException e) {
                throw new IllegalArgumentException(
                        "Unable to parse probe metadata input JSON.", e);
            }

            final StringReader reader = new StringReader(diagsLines.get(DIAGS_ID_SVC_IDX));
            identityService.restore(reader);
        }
        logger.info("Successfully restored the Identity Provider!");
    }
}
