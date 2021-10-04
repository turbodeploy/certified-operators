package com.vmturbo.topology.processor.history;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.AbstractMessage;

import io.grpc.stub.AbstractStub;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.components.common.utils.AutoCloseables;
import com.vmturbo.components.common.utils.ThrowingFunction;
import com.vmturbo.platform.common.dto.CommonDTO.NotificationDTO;
import com.vmturbo.platform.common.dto.CommonDTO.NotificationDTO.Severity;
import com.vmturbo.platform.sdk.common.util.NotificationCategoryDTO;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;

/**
 * Abstract class that persists cache state as serialized protobuf blobs.
 *
 * @param <DbValueT> the pre-calculated element of per-field data as retrieved from the persistent store
 * @param <HistoryDataT> per-commodity field historical data to cache that wraps DbValue with runtime info
 * @param <HistoryLoadingTaskT> loader of DbValue's from the persistent store
 * @param <ConfigT> per-editor type configuration values holder
 * @param <StubT> type of history component stub
 * @param <CheckpointResultT> the result of checkpoint, if applicable.
 * @param <SerializedBlobBuilderT> The serialized protobuf blob type.
 */
public abstract class BlobPersistingCachingHistoricalEditor<HistoryDataT extends IHistoryCommodityData<ConfigT, DbValueT, CheckpointResultT>,
    HistoryLoadingTaskT extends IHistoryLoadingTask<ConfigT, DbValueT>,
    ConfigT extends CachingHistoricalEditorConfig,
    DbValueT,
    StubT extends AbstractStub<StubT>,
    CheckpointResultT,
    SerializedBlobBuilderT extends AbstractMessage.Builder<SerializedBlobBuilderT>>
    extends AbstractCachingHistoricalEditor<HistoryDataT, HistoryLoadingTaskT, ConfigT, DbValueT, StubT, CheckpointResultT> {

    private static final Logger logger = LogManager.getLogger();

    private final Clock clock;
    private final SystemNotificationProducer systemNotificationProducer;
    private final IdentityProvider identityProvider;

    /**
     * Construct the instance of a caching history editor.
     *
     * @param config                    per-type configuration
     * @param statsHistoryClient        history db client
     * @param historyLoadingTaskCreator create an instance of a db value loading task
     * @param historyDataCreator        create an instance of cached history element
     * @param clock                     the clock to use for timing
     * @param systemNotificationProducer system notification producer
     * @param identityProvider          the identity provider used to get existing oids
     */
    protected BlobPersistingCachingHistoricalEditor(@Nullable ConfigT config,
                                                    @Nonnull StubT statsHistoryClient,
                                                    @Nonnull BiFunction<StubT, Pair<Long, Long>, HistoryLoadingTaskT> historyLoadingTaskCreator,
                                                    @Nonnull Supplier<HistoryDataT> historyDataCreator,
                                                    @Nonnull final Clock clock,
                                                    @Nonnull SystemNotificationProducer systemNotificationProducer,
                                                    @Nonnull IdentityProvider identityProvider) {
        super(config, statsHistoryClient, historyLoadingTaskCreator, historyDataCreator);
        this.clock = clock;
        this.systemNotificationProducer = systemNotificationProducer;
        this.identityProvider = identityProvider;
    }

    /**
     * Get the clock associated with the editor.
     *
     * @return the clock associated with the editor.
     */
    public Clock getClock() {
        return clock;
    }

    /**
     * Get a mapping of the state-exporting functions used when exporting diagnostics. The String
     * first element of the pair should be the name of the diagnostics file
     * (including the file extension suffix). The second element of the pair should be a function
     * that, given a {@link HistoryDataT} commodty data object, generates the serialized protobuf
     * checkpoint record.
     *
     * @return a mapping of the state-exporting functions used when exporting diagnostics. The mapping
     *         is from file-name to state-exporting function.
     */
    @Nonnull
    protected abstract List<Pair<String, ThrowingFunction<HistoryDataT, CheckpointResultT, HistoryCalculationException>>>
        getStateExportingFunctions();

    /**
     * Create the protobuf blob for persistence.
     *
     * @param dumpingFunction The state-exporting function to generate protobuf objects for persistence (blobs).
     * @return The builder for the protobuf blob.
     * @throws HistoryCalculationException If data cannot be serialized to the blob.
     * @throws InterruptedException If interrupted.
     */
    @Nonnull
    protected abstract SerializedBlobBuilderT createBlob(
        @Nonnull ThrowingFunction<HistoryDataT, CheckpointResultT, HistoryCalculationException> dumpingFunction)
        throws HistoryCalculationException, InterruptedException;

    /**
     * Get the number of individual records in the serialized blob.
     *
     * @param blobBuilder The builder for the protobuf blob.
     * @return the number of individual records in the serialized blob.
     */
    protected abstract int getRecordCount(@Nonnull SerializedBlobBuilderT blobBuilder);

    /**
     * Restore persisted data from the input streams into the editor's cache. Used when
     * restoring diagnostics data.
     *
     * @param diagsMapping Mapping from individual diags file names to their content loaded in
     *                    input streams (containing persisted blobs)
     * @param context invocation context i.e current graph
     * @throws HistoryCalculationException If data cannot be restored (ie due to corruption or other reasons)
     * @throws InterruptedException If interrupted
     * @throws IOException If there is an error reading from or closing the input streams.
     */
    protected abstract void restorePersistedData(@Nonnull Map<String, InputStream> diagsMapping,
                                                 @Nonnull HistoryAggregationContext context)
        throws HistoryCalculationException, InterruptedException, IOException;

    @Override
    public synchronized void initContext(@Nonnull HistoryAggregationContext context,
                                         @Nonnull List<EntityCommodityReference> eligibleComms)
        throws HistoryCalculationException, InterruptedException {
        super.initContext(context, eligibleComms);

        initializeCacheValues(context, eligibleComms);
    }

    /**
     * Creates a task to load history data.
     *
     * @param startTimestamp the start timestamp
     * @return the constructed history-loading task
     */
    protected HistoryLoadingTaskT createTask(long startTimestamp) {
        return createLoadingTask(Pair.create(startTimestamp, null));
    }

    @Override
    protected void exportState(@Nonnull final OutputStream appender)
        throws DiagnosticsException, IOException {
        try (ByteArrayOutputStream diagsOutput = new ByteArrayOutputStream()) {
            // Percentile has two diag files containing full and latest counts will be compressed in its own zip
            // Moving statistics only has a single zip containing all moving statistics records.
            try (ZipOutputStream zos = new ZipOutputStream(diagsOutput)) {
                for (Pair<String, ThrowingFunction<HistoryDataT, CheckpointResultT, HistoryCalculationException>>
                    exporter : getStateExportingFunctions()) {
                    final String zipEntryName = exporter.getFirst();
                    final ThrowingFunction<HistoryDataT, CheckpointResultT, HistoryCalculationException> serializationFunction =
                        exporter.getSecond();

                    final int recordsDumped = dumpDiagsToZipEntry(zos, zipEntryName, serializationFunction);
                    logger.info("Dumped {} cache record(s) in '{}' diagnostic file.",
                        () -> recordsDumped, () -> zipEntryName);
                }
            }
            diagsOutput.writeTo(appender);
        } catch (HistoryCalculationException e) {
            throw new DiagnosticsException(String.format("Cannot write blob cache into '%s' file",
                getFileName()));
        } catch (InterruptedException e) {
            // Re-interrupt the current thread so that the caller knows that it has to
            // interrupt w/e it's doing as well
            Thread.currentThread().interrupt();
            throw new DiagnosticsException(String.format("Thread interrupted while writing blob cache into '%s' file",
                getFileName()));
        }
    }

    @Override
    protected void restoreState(@Nonnull final byte[] bytes) throws DiagnosticsException {
        final Map<String, InputStream> diagsMapping = getDiagsMapping(bytes);
        final HistoryAggregationContext context = createEmptyContext();

        try (AutoCloseables<DiagnosticsException> resources = new AutoCloseables<>(diagsMapping.values(),
            (topLevelException) -> new DiagnosticsException(
                "One or more exceptions while restoring diagnostics", topLevelException))) {
            restorePersistedData(diagsMapping, context);
        } catch (HistoryCalculationException | InterruptedException | IOException e) {
            if (e instanceof InterruptedException) {
                // Re-interrupt the current thread so that the caller knows that it has to
                // interrupt w/e it's doing as well
                Thread.currentThread().interrupt();
            }

            getCache().clear();
            throw new DiagnosticsException(
                String.format("Cannot load %s cache from '%s' file",
                    getClass().getSimpleName(), getFileName()), e);
        }
    }

    /**
     * Data is loaded in a single-threaded way into blobs (not chunked).
     * Initialize cache values for entries from topology in order to get
     * the data for them calculated as well if they do not get read.
     *
     * @param context The aggregation context
     * @param commodityRefs The commodity refs from the cache to initialize
     */
    protected void initializeCacheValues(@Nonnull HistoryAggregationContext context,
                                         @Nonnull Collection<? extends EntityCommodityReference> commodityRefs) {
        commodityRefs.forEach(commRef -> {
            EntityCommodityFieldReference field =
                (new EntityCommodityFieldReference(commRef, CommodityField.USED))
                    .getLiveTopologyFieldReference(context);
            getCache().computeIfAbsent(field, fieldRef -> {
                HistoryDataT data = historyDataCreator.get();
                data.init(field, null, getConfig(), context);
                return data;
            });
        });
    }

    /**
     * Remove all the oids in the cache that are not contained in the identity cache.
     *
     * @throws HistoryCalculationException is oids can't be read from the identity cache
     */
    protected void expireStaleOidsFromCache() throws HistoryCalculationException {
        Set<Long> currentOidsInIdentityCache = getCurrentOidsInInIdentityCache();
        int originalCacheSize = getCache().keySet().size();
        getCache().keySet().removeIf(entityRef -> !currentOidsInIdentityCache.contains(entityRef.getEntityOid()));
        int numEntriesExpired = originalCacheSize - getCache().keySet().size();
        Level level = numEntriesExpired > 0 ? Level.INFO : Level.DEBUG;
        logger.log(level, "{} expired oids were filtered out during maintenance",
            numEntriesExpired);
    }

    /**
     * Get the current OIDs in the identity provider.
     *
     * @return A set containing the OIDs of the entities in the identity cache.
     * @throws HistoryCalculationException If the identity provider has not been properly initialized.
     */
    protected Set<Long> getCurrentOidsInInIdentityCache()
        throws HistoryCalculationException {
        try {
            return identityProvider.getCurrentOidsInIdentityCache();
        } catch (IdentityUninitializedException e) {
            throw new HistoryCalculationException("Could not get existing oids from the "
                + "identity cache", e);
        }
    }

    /**
     * Send a notification of an event to the systemNotificationSender.
     *
     * @param event The event for which we are generating the notification.
     * @param description A description of what happened.
     * @param severity The severity fo the event.
     */
    protected void sendNotification(@Nonnull String event, @Nonnull String description,
            @Nonnull Severity severity) {
        systemNotificationProducer.sendSystemNotification(
                Collections.singletonList(NotificationDTO.newBuilder()
                        .setEvent(event)
                        .setSeverity(severity)
                        .setCategory(NotificationCategoryDTO.NOTIFICATION.name())
                        .setDescription(description)
                        .build()), null);
    }

    /**
     * Dump historical data blob to diags zip entry.
     *
     * @param zos zip output stream to append
     * @param zipEntryName Zip entry name
     * @param dumpingFunction Function to generate serialized blob data
     *
     * @return Number of records dumped
     * @throws IOException when failed to write zip entry
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    private int dumpDiagsToZipEntry(@Nonnull final ZipOutputStream zos,
                                    @Nonnull final String zipEntryName,
                                    @Nonnull ThrowingFunction<HistoryDataT, CheckpointResultT, HistoryCalculationException> dumpingFunction)
        throws IOException, InterruptedException, HistoryCalculationException {
        final ZipEntry ze = new ZipEntry(zipEntryName);
        ze.setTime(getClock().millis());
        zos.putNextEntry(ze);
        final int size = dumpHistoryData(zos, dumpingFunction);
        zos.closeEntry();
        return size;
    }

    private int dumpHistoryData(@Nonnull OutputStream output,
                                @Nonnull ThrowingFunction<HistoryDataT, CheckpointResultT, HistoryCalculationException> dumpingFunction)
        throws HistoryCalculationException, InterruptedException, IOException {
        final SerializedBlobBuilderT blobBuilder = createBlob(dumpingFunction);
        blobBuilder.build().writeDelimitedTo(output);
        return getRecordCount(blobBuilder);
    }

    @Nonnull
    private HistoryAggregationContext createEmptyContext() {
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder().setTopologyId(
            getConfig().getRealtimeTopologyContextId()).build();
        final Map<Integer, Collection<TopologyEntity>> typeToIndex = new HashMap<>();
        final Long2ObjectMap<TopologyEntity> oidToEntity = new Long2ObjectOpenHashMap<>();
        final TopologyGraph<TopologyEntity> graph = new TopologyGraph<>(oidToEntity, typeToIndex);
        final Map<Long, EntitySettings> oidToSettings = new HashMap<>();
        final Map<Long, SettingPolicy> defaultPolicies = new HashMap<>();
        final GraphWithSettings graphWithSettings = new GraphWithSettings(graph, oidToSettings,
            defaultPolicies);
        return new HistoryAggregationContext(topologyInfo, graphWithSettings, false);
    }

    /**
     * Get mapping of sub-diag-file names to InputStream to read those bits.
     *
     * @param compressedDiags Blob diags zip
     * @return combined diags (full + latest) to be consumed by the loading logic
     * @throws DiagnosticsException on error.
     */
    @Nonnull
    protected Map<String, InputStream> getDiagsMapping(@Nonnull final byte[] compressedDiags) throws DiagnosticsException {
        final Map<String, byte[]> nameToBytes = new HashMap<>();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressedDiags)) {
            final Iterable<Diags> diagsReader = new DiagsZipReader(bais, null, true);
            diagsReader.iterator().forEachRemaining(diags -> {
                nameToBytes.putIfAbsent(diags.getName(), diags.getBytes());
            });

            final Map<String, InputStream> nameToInputStream = new HashMap<>();
            for (Pair<String, ThrowingFunction<HistoryDataT, CheckpointResultT, HistoryCalculationException>>
                exporterPair : getStateExportingFunctions()) {
                final String subFileName = exporterPair.getFirst();
                try (ByteArrayInputStream inStream = new ByteArrayInputStream(getSubDiags(nameToBytes, subFileName))) {
                    nameToInputStream.put(subFileName, inStream);
                }
            }
            return nameToInputStream;
        } catch (IOException e) {
            throw new DiagnosticsException("Unable to read compressed diags byts", e);
        }
    }

    /**
     * Get diags bits of the specified type.
     *
     * @param diagsMapping mapping of diags name to diags content
     * @param fileName file name of subdiags requested
     * @return requested subdiags content
     * @throws DiagnosticsException if requested subdiags are missing
     */
    @Nonnull
    private byte[] getSubDiags(@Nonnull final Map<String, byte[]> diagsMapping,
                               @Nonnull final String fileName)
        throws DiagnosticsException {
        final byte[] subDiags = diagsMapping.getOrDefault(fileName, null);
        if (subDiags == null) {
            throw new DiagnosticsException(
                String.format("Cannot load %s cache from '%s' file due to missing %s part",
                    getClass().getSimpleName(), getFileName(), fileName));
        }
        return subDiags;
    }
}
