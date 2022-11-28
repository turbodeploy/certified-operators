package com.vmturbo.history.ingesters.live.writers;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase.IngesterState;
import com.vmturbo.history.ingesters.common.writers.TopologyWriterBase;
import com.vmturbo.history.notifications.ApplicationServiceHistoryNotificationSender;
import com.vmturbo.history.schema.abstraction.tables.ApplicationServiceDaysEmpty;
import com.vmturbo.history.schema.abstraction.tables.records.ApplicationServiceDaysEmptyRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Writer responsible for inserting data into the application service days empty table.
 */
public class ApplicationServiceDaysEmptyWriter extends TopologyWriterBase {

    private final Logger logger = LogManager.getLogger();
    private static final ApplicationServiceDaysEmpty ASDE = ApplicationServiceDaysEmpty.APPLICATION_SERVICE_DAYS_EMPTY;
    private final BulkLoader<ApplicationServiceDaysEmptyRecord> emptyAppSvcLoader;
    private final TopologyInfo topologyInfo;
    private final ApplicationServiceHistoryNotificationSender appSvcHistorySender;
    private LongSet emptyAppSvcs = new LongArraySet();
    private LongSet nonEmptyAppSvcs = new LongArraySet();
    private Map<Long, String> namesById = new HashMap<>();
    private DSLContext dslContext;

    protected ApplicationServiceDaysEmptyWriter(@Nonnull TopologyInfo topologyInfo,
            @Nonnull SimpleBulkLoaderFactory loaderFactory,
            @Nonnull ApplicationServiceHistoryNotificationSender appSvcHistorySender,
            @Nonnull DSLContext dslContext) {
        // Loader for empty app service plans will exclude updating the discovered_empty field
        this.emptyAppSvcLoader = loaderFactory.getLoader(ASDE, Collections.singleton(ASDE.FIRST_DISCOVERED));
        this.topologyInfo = topologyInfo;
        this.appSvcHistorySender = appSvcHistorySender;
        this.dslContext = dslContext;
    }

    @Override
    protected ChunkDisposition processEntities(@Nonnull final Collection<TopologyEntityDTO> chunk,
            @Nonnull final String infoSummary)
            throws InterruptedException {
        List<TopologyEntityDTO> vmSpecChunk = chunk.stream()
                .filter(e -> e.getEntityType() == EntityType.VIRTUAL_MACHINE_SPEC_VALUE)
                .collect(Collectors.toList());
        for (TopologyEntityDTO vmSpec : vmSpecChunk) {
            namesById.put(vmSpec.getOid(), vmSpec.getDisplayName());
            int appCount = getAppCount(vmSpec);
            if (appCount == 0) {
                emptyAppSvcs.add(vmSpec.getOid());
            } else {
                nonEmptyAppSvcs.add(vmSpec.getOid());
            }
        }
        return ChunkDisposition.SUCCESS;
    }

    private int getAppCount(TopologyEntityDTO vmSpec) {
        if (vmSpec.hasTypeSpecificInfo() && vmSpec.getTypeSpecificInfo().hasApplicationService()) {
            return vmSpec.getTypeSpecificInfo().getApplicationService().getAppCount();
        }
        return 0;
    }

    private List<ApplicationServiceDaysEmptyRecord> createRecords(final Set<Long> appSvcOids) {
        Timestamp tpCreationTime = new Timestamp(topologyInfo.getCreationTime());
        return appSvcOids.stream().map(
                        oid -> new ApplicationServiceDaysEmptyRecord(oid,
                                namesById.get(oid),
                                tpCreationTime,
                                tpCreationTime))
            .collect(Collectors.toList());
    }

    @Override
    public void finish(int entityCount, boolean expedite, String infoSummary)
            throws InterruptedException {
        int deletionCount = deleteAppServicesWithApps();
        emptyAppSvcLoader.insertAll(createRecords(emptyAppSvcs));
        emptyAppSvcLoader.flush(true);
        logger.info("Finished entity processing, app services={}, empty app svc={}, "
                        + "non-empty-count={}, deleted app svcs={}",
                emptyAppSvcs.size() + nonEmptyAppSvcs.size(), emptyAppSvcs.size(),
                nonEmptyAppSvcs.size(), deletionCount);
        appSvcHistorySender.sendDaysEmptyNotification(topologyInfo.getTopologyId());
    }

    @VisibleForTesting
    protected int deleteAppServicesWithApps() {
        // An app service plan that had no apps can later have apps added to it.
        // We need to delete such app services from the days empty table.
        return this.dslContext.deleteFrom(ASDE)
                .where(ASDE.ID.in(nonEmptyAppSvcs))
                .execute();
    }

    /**
     * Factory for ApplicationServiceAppHistoryWriter.
     */
    public static class Factory extends TopologyWriterBase.Factory {

        private final Logger logger = LogManager.getLogger();
        private final long appSvcDaysEmptyUpdateIntervalInMinutes;
        private long lastInsertTime;
        private final ApplicationServiceHistoryNotificationSender appSvcHistorySender;
        private DSLContext dslContext;

        /**
         * Creates a Factory for ApplicationServiceAppHistoryWriter.
         *
         * @param appSvcDaysEmptyUpdateIntervalInMinutes interval between db updates
         * @param appSvcHistorySender the app service history sender
         */
        public Factory(final long appSvcDaysEmptyUpdateIntervalInMinutes,
                @Nonnull final ApplicationServiceHistoryNotificationSender appSvcHistorySender,
                DSLContext dslContext) {
            this.appSvcDaysEmptyUpdateIntervalInMinutes = appSvcDaysEmptyUpdateIntervalInMinutes;
            this.appSvcHistorySender = appSvcHistorySender;
            this.dslContext = dslContext;
        }

        @Override
        public Optional<IChunkProcessor<DataSegment>> getChunkProcessor(
                final TopologyInfo topologyInfo,
                final IngesterState state) {
            if (appSvcDaysEmptyUpdateIntervalInMinutes < 0) {
                logger.info("ApplicationServiceAppHistoryWriter is disabled, "
                        + "appServiceDaysEmptyUpdateIntervalInMinutes={}", appSvcDaysEmptyUpdateIntervalInMinutes);
            }
            final long intervalBetweenUpdatesMs = TimeUnit.MINUTES.toMillis(appSvcDaysEmptyUpdateIntervalInMinutes);
            final long currTopologyCreationTime = topologyInfo.getCreationTime();
            final long nextInsertTime = lastInsertTime + intervalBetweenUpdatesMs;
            if (currTopologyCreationTime >= nextInsertTime) {
                lastInsertTime = currTopologyCreationTime;
                logger.info("{} will begin chunk processing "
                        + "VirtualMachineSpec entities for days empty, appServiceDaysEmptyUpdateIntervalInMinutes={}",
                        ApplicationServiceDaysEmptyWriter.class.getSimpleName(),
                        appSvcDaysEmptyUpdateIntervalInMinutes);
                return Optional.of(
                        new ApplicationServiceDaysEmptyWriter(
                                topologyInfo,
                                state.getLoaders(),
                                appSvcHistorySender,
                                dslContext)
                );
            } else {
                if (logger.isDebugEnabled()) {
                    long timeToNext = nextInsertTime - currTopologyCreationTime;
                    long timeToNextMinutes = TimeUnit.MILLISECONDS.toMinutes(timeToNext);
                    long intervalInMinutes = TimeUnit.MILLISECONDS.toMinutes(intervalBetweenUpdatesMs);
                    logger.debug("Skipping days empty upsert:"
                                    + "\n minutes until next insert: {}"
                                    + "\n interval between inserts: {}"
                                    + "\n current topology time: {}, "
                                    + "\n next insert time: {}, ",
                            timeToNextMinutes, intervalInMinutes, currTopologyCreationTime, nextInsertTime);
                }
                return Optional.empty();
            }
        }
    }
}
