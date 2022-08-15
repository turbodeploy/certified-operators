package com.vmturbo.cost.component.savings;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionChangeWindowRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionChangeWindowResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.UpdateActionChangeWindowRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.UpdateActionChangeWindowRequest.ActionLivenessInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.UpdateActionChangeWindowResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;

/**
 * Cache impl of action store.
 */
public class CachedSavingsActionStore implements SavingsActionStore {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Lock for cache and related structures.
     */
    private final ReadWriteLock cacheLock = new ReentrantReadWriteLock();

    /**
     * AO interface.
     */
    private final ActionsServiceBlockingStub actionsServiceBlockingStub;

    /**
     * Cache of change window instances by action id.
     */
    @GuardedBy("cacheLock")
    private final Map<Long, ExecutedActionsChangeWindow> cacheByActionId;

    /**
     * Action ids that we know are 'dirty', meaning we need to query AO and get their latest state.
     * These are executed actions that we got notified on via ExecutedActionsListener, and we
     * need to query AO to get latest state info about these actions.
     */
    @GuardedBy("cacheLock")
    private final Map<Long, Long> dirtyActions;

    /**
     * Set of changes that need to be committed, done during the post-processing step.
     */
    @GuardedBy("cacheLock")
    private final List<ActionLivenessInfo> stagingChanges;

    /**
     * Time when full cache entries were hard refreshed last time.
     */
    @GuardedBy("cacheLock")
    private LocalDateTime lastRefreshedTime = null;

    /**
     * Whether to enable option to periodically dump the cache contents.
     */
    private final boolean dumpCacheEnabled;

    /**
     * Format of date in dump file name.
     */
    private final DateTimeFormatter dumpFileDateFormat = DateTimeFormatter.ofPattern("dd.MM_HH.mm");

    /**
     * Path to the dump file.
     */
    private static final String dumpFilePathFormat = "/tmp/savings_action_cache_%s_%s.json";

    /**
     * UTC clock.
     */
    private final Clock clock;

    /**
     * How often to do a hard cache refresh.
     */
    private final long cacheDurationHours;

    /**
     * How big of changes to process for updates.
     */
    private final int chunkSize;

    /**
     * How much initial delay to use for initializer thread.
     */
    private final long initializerDelayMinutes;

    /**
     * How often to run the initializer thread.
     */
    private final long initializerPeriodMinutes;

    /**
     * States that we are tracking.
     */
    List<LivenessState> trackedLivenessStates = ImmutableList.of(
            LivenessState.LIVE,
            LivenessState.NEW);


    /**
     * Create new instance.
     *
     * @param stub AO stub.
     * @param clock Clock.
     * @param cacheDurationHours How often to refresh cache from AO.
     * @param processingChunkSize Size of batch for AO update requests.
     * @param initializerDelayMinutes How much initial delay to use for initializer thread.
     * @param initializerPeriodMinutes How often to run the initializer thread.
     * @param dumpCache Dump cache option for diagnostics.
     */
    public CachedSavingsActionStore(@Nonnull final ActionsServiceBlockingStub stub,
            @Nonnull final Clock clock, long cacheDurationHours, int processingChunkSize,
            long initializerDelayMinutes, long initializerPeriodMinutes, boolean dumpCache) {
        this.cacheByActionId = new HashMap<>();
        this.dirtyActions = new HashMap<>();
        this.actionsServiceBlockingStub = stub;
        this.clock = clock;
        this.cacheDurationHours = cacheDurationHours;
        this.stagingChanges = new ArrayList<>();
        this.chunkSize = processingChunkSize;
        this.initializerDelayMinutes = initializerDelayMinutes;
        this.initializerPeriodMinutes = initializerPeriodMinutes;
        this.dumpCacheEnabled = dumpCache;
    }

    /**
     * Called when store is initialized for the first time on creation. Starts up background
     * initializer thread that periodically checks if cache is in sync and tries to sync it if not.
     *
     * @param useSchedule Uses a scheduled thread to init, default option. False only for testing.
     */
    void initialize(boolean useSchedule) {
        if (useSchedule) {
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::load,
                    initializerDelayMinutes, initializerPeriodMinutes, TimeUnit.MINUTES);
            logger.info("Created savings cache initializer to run after {} mins, every {} mins, "
                            + "with duration {} hours.", initializerDelayMinutes,
                    initializerPeriodMinutes, cacheDurationHours);
        } else {
            load();
        }
    }

    /**
     * Whether cache has been successfully initialized and is usable now.
     * If either we never refreshed before, or had some critical error had needs a full refresh,
     * or if it has been longer than 24 hours since refresh, then we do a hard refresh.
     *
     * @return False if a full refresh is required and cache is currently unusable.
     */
    private boolean checkReady() {
        cacheLock.readLock().lock();
        try {
            if (lastRefreshedTime == null) {
                // Never initialized.
                return false;
            }
            final LocalDateTime currentTime = LocalDateTime.now(clock);
            if (ChronoUnit.HOURS.between(lastRefreshedTime, currentTime) >= cacheDurationHours) {
                // Too long since last full refresh, so needs to be re-initialized.
                return false;
            }
            if (dirtyActions.isEmpty()) {
                // No dirty actions to sync up, so we are all ready.
                return true;
            }
        } finally {
            cacheLock.readLock().unlock();
        }
        // We have some dirty actions to sync up, so do it now.
        try {
            clearDirtyActions();
        } catch (SavingsException se) {
            logger.warn("Unable to clear dirty action items for cache.", se);

            // Force full cache refresh next time.
            clearLastRefreshed();

            // Not ready to use cache, as we are not in sync with AO.
            return false;
        }
        // All in sync, cache is ready to use.
        return true;
    }

    @Nonnull
    @Override
    public Set<ExecutedActionsChangeWindow> getActions(@Nonnull final LivenessState state)
            throws SavingsException {
        if (!checkReady()) {
            throw new SavingsException("Cache not ready to be used yet.");
        }
        cacheLock.readLock().lock();
        try {
            return cacheByActionId.values()
                    .stream()
                    .filter(changeWindow -> changeWindow.getLivenessState() == state)
                    .collect(Collectors.toSet());
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    @Nonnull
    @Override
    public Optional<ExecutedActionsChangeWindow> getAction(long actionId)
            throws SavingsException {
        if (!checkReady()) {
            throw new SavingsException("Cache not ready to be used yet.");
        }
        cacheLock.readLock().lock();
        try {
            return Optional.ofNullable(cacheByActionId.get(actionId));
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    @Override
    public void activateAction(long actionId, long timestamp)
            throws SavingsException {
        if (!checkReady()) {
            throw new SavingsException("Cache not ready to be used yet.");
        }
        cacheLock.writeLock().lock();
        try {
            updateAction(actionId, timestamp, LivenessState.LIVE);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    @Override
    public void deactivateAction(long actionId, long endTime, LivenessState state)
            throws SavingsException {
        if (!checkReady()) {
            throw new SavingsException("Cache not ready to be used yet.");
        }
        cacheLock.writeLock().lock();
        try {
            updateAction(actionId, endTime, state);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    @Override
    public void saveChanges() throws SavingsException {
        tracePrintCache(String.format("Saving %s staging changes to cache.", stagingChanges.size()));
        if (stagingChanges.isEmpty()) {
            // No new changes to save.
            return;
        }
        int stagingCount = stagingChanges.size();
        cacheLock.writeLock().lock();
        try {
            // Verify the change are all valid, i.e. are being made to actions that are known to cache.
            final Set<Long> missingActions = stagingChanges
                    .stream()
                    .map(ActionLivenessInfo::getActionOid)
                    .filter(actionOid -> !cacheByActionId.containsKey(actionOid))
                    .collect(Collectors.toSet());
            if (!missingActions.isEmpty()) {
                throw new SavingsException("Missing following action ids in cache (size: "
                        + cacheByActionId.size() + "): " + missingActions);
            }
            logger.info("Updating {} action changes to cache (size: {}).", stagingCount,
                    cacheByActionId.size());
            // Save changes to AO first.
            saveToStore();

            // Save to cache.
            saveToCache();
            logger.info("Completed {} action changes to cache (size: {}).", stagingCount,
                    cacheByActionId.size());
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    @Override
    public void onNewAction(long actionId, long entityId) {
        cacheLock.writeLock().lock();
        try {
            dirtyActions.put(actionId, entityId);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    /**
     * Only for testing, count of currently dirty actions.
     *
     * @return Dirty action count.
     */
    @VisibleForTesting
    int countDirty() {
        cacheLock.readLock().lock();
        try {
            return dirtyActions.size();
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    /**
     * Marks the whole cache as dirty, so that we can do a full refresh by going to AO and fetching
     * all records.
     */
    private void clearLastRefreshed() {
        cacheLock.writeLock().lock();
        try {
            if (lastRefreshedTime != null) {
                logger.info("Clearing action liveness last refreshed timestamp: {}.",
                        lastRefreshedTime);
                lastRefreshedTime = null;
            }
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    /**
     * This gets called via the periodic cache initializer thread.
     */
    private void load() {
        tracePrintCache("Checking loading from store.");
        if (checkReady()) {
            return;
        }
        // Doing a full cache refresh as we are out of sync.
        tracePrintCache("Going to do full cache refresh.");
        cacheLock.writeLock().lock();
        try {
            final LocalDateTime currentTime = LocalDateTime.now(clock);
            dumpCache("before", lastRefreshedTime == null ? currentTime : lastRefreshedTime);

            clearLastRefreshed();
            cacheByActionId.clear();
            dirtyActions.clear();

            queryStore(Collections.emptySet(), rec -> cacheByActionId.put(rec.getActionOid(), rec));
            lastRefreshedTime = currentTime;
            logger.info("Successfully refreshed full action window cache (size: {}) at {}.",
                    cacheByActionId.size(), lastRefreshedTime);

            dumpCache("after", lastRefreshedTime);
        } catch (SavingsException se) {
            logger.error("Unable to do full action window cache refresh.", se);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    /**
     * Dumps cache contents to output tmp file if enabled.
     * Writes a file like: /tmp/savings_action_cache_after_27.05_13.12.json
     *
     * @param filePrefix Prefix to use for filename.
     * @param time Timestamp to use in filename.
     */
    private void dumpCache(@Nonnull final String filePrefix, @Nonnull final LocalDateTime time) {
        if (!dumpCacheEnabled || !logger.isTraceEnabled()) {
            return;
        }
        final String filename = String.format(dumpFilePathFormat, filePrefix,
                dumpFileDateFormat.format(time));
        // Sort by action ids so that it is easier to find/compare in dump files.
        final TreeSet<Long> sortedActionIds = new TreeSet<>(cacheByActionId.keySet());
        try {
            final File jsonFile = new File(filename);
            // Partition in chunks and write a chunk at a time, to avoid converting all to json
            // in one shot. Write prefix and suffix for json array wrapper.
            FileUtils.writeStringToFile(jsonFile,
                    "[" + System.lineSeparator(),
                    StandardCharsets.UTF_8, true);
            for (List<Long> chunkActionIds : ListUtils.partition(new ArrayList<>(sortedActionIds),
                    chunkSize)) {
                for (Long actionId : chunkActionIds) {
                    FileUtils.writeStringToFile(jsonFile,
                            JsonFormat.printer().print(cacheByActionId.get(actionId))
                                    + "," + System.lineSeparator(),
                            StandardCharsets.UTF_8, true);
                }
            }
            FileUtils.writeStringToFile(jsonFile,
                    "]" + System.lineSeparator(),
                    StandardCharsets.UTF_8, true);
        } catch (InvalidProtocolBufferException ipbe) {
            logger.warn("Unable to convert action window to JSON file: {}.", filename, ipbe);
        } catch (IOException ioe) {
            logger.warn("Unable to write action window to JSON file: {}.", filename, ioe);
        }
    }

    /**
     * Helper method to trace print cache details.
     *
     * @param message Extra message to print.
     */
    private void tracePrintCache(@Nonnull final String message) {
        if (!logger.isTraceEnabled()) {
            return;
        }
        cacheLock.readLock().lock();
        try {
            logger.trace("{}: Cache size: {}, Dirty actions: {}, Last refreshed: {}.",
                    message, cacheByActionId.size(), dirtyActions.size(), lastRefreshedTime);
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    /**
     * Query any dirty entries from AO and sync the local cache, and then clear the dirty list.
     * After this, we are in sync. Assumes that cache has already been initialized.
     */
    private void clearDirtyActions() throws SavingsException {
        cacheLock.readLock().lock();
        try {
            if (dirtyActions.isEmpty()) {
                // Nothing to clear, cache is all in sync.
                return;
            }
        } finally {
            cacheLock.readLock().unlock();
        }

        // Need to sync some dirty items. Done it in a chunk at a time. Clear dirty after done.
        cacheLock.writeLock().lock();
        try {
            final List<List<Long>> subListChanges = ListUtils.partition(new ArrayList<>(
                    dirtyActions.keySet()), chunkSize);
            for (List<Long> eachSubList : subListChanges) {
                final Set<Long> actionIds = new HashSet<>(eachSubList);
                final List<ExecutedActionsChangeWindow> actionRecords = new ArrayList<>();
                queryStore(actionIds, actionRecords::add);
                actionRecords.forEach(rec -> cacheByActionId.put(rec.getActionOid(), rec));
            }
            dirtyActions.clear();
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    /**
     * Makes query to AO to get info about action change windows. Either gets all or only the
     * specified ones. Only LIVE and NEW records are fetched.
     *
     * @param actionIds Optionally specify action ids to query for. If empty, then we get all.
     * @param consumer Consumer that gets notified of results.
     * @throws SavingsException Thrown on fetch related errors.
     */
    private void queryStore(@Nonnull final Set<Long> actionIds,
            @Nonnull final Consumer<ExecutedActionsChangeWindow> consumer)
            throws SavingsException {
        final GetActionChangeWindowRequest.Builder requestBuilder = GetActionChangeWindowRequest
                .newBuilder().addAllLivenessStates(trackedLivenessStates);
        if (!actionIds.isEmpty()) {
            requestBuilder.addAllActionOids(new ArrayList<>(actionIds));
        }
        GetActionChangeWindowRequest request;
        Optional<Long> nextCursor = Optional.empty();
        do {
            final long currentCursor = nextCursor.isPresent() ? nextCursor.get() : 0;
            request = requestBuilder.setPaginationParams(PaginationParameters.newBuilder()
                            .setCursor(Long.toString(currentCursor))
                            .build())
                    .build();
            GetActionChangeWindowResponse response;
            try {
                response = actionsServiceBlockingStub.getActionChangeWindows(request);
            } catch (StatusRuntimeException sre) {
                final String message = String.format("Error fetching action windows for request: %s",
                        request);
                logger.debug(message, sre);
                throw new SavingsException(message, sre);
            }
            response.getChangeWindowsList().forEach(consumer);
            if (response.hasPaginationResponse() && response.getPaginationResponse()
                        .hasNextCursor()) {
                final String cursorString = response.getPaginationResponse().getNextCursor();
                try {
                    nextCursor = Optional.of(Long.parseLong(cursorString));
                } catch (NumberFormatException nfe) {
                    final String message = String.format("Parse error for action window "
                            + "response %s, for request: %s", cursorString, request);
                    logger.debug(message, nfe);
                    throw new SavingsException(message, nfe);
                }
            } else {
                nextCursor = Optional.empty();
            }
        } while (nextCursor.isPresent());
    }

    /**
     * Saves outstanding changes to AO store. E.g. a change to convert an action from NEW to LIVE.
     * Updates are made in chunks at a time.
     *
     * @throws SavingsException Thrown on update error.
     */
    private void saveToStore() throws SavingsException {
        try {
            final List<List<ActionLivenessInfo>> subListChanges = ListUtils.partition(stagingChanges,
                    chunkSize);
            for (List<ActionLivenessInfo> eachSubList : subListChanges) {
                final UpdateActionChangeWindowResponse response = actionsServiceBlockingStub
                        .updateActionChangeWindows(UpdateActionChangeWindowRequest.newBuilder()
                                .addAllLivenessInfo(eachSubList)
                                .build());
            }
        } catch (StatusRuntimeException ex) {
            final String message = String.format("Unable to save %s action window changes.",
                    stagingChanges.size());
            logger.debug(message, ex);
            // If we have a problem reaching AO, that means cache is not reliable, we need a full refresh.
            clearLastRefreshed();
            throw new SavingsException(message, ex);
        }
    }

    /**
     * Saves staging changes to the cache. This is done only after AO has been successfully updated.
     */
    private void saveToCache() {
        for (ActionLivenessInfo livenessInfo : stagingChanges) {
            long actionId = livenessInfo.getActionOid();
            LivenessState state = livenessInfo.getLivenessState();
            final long timestamp = livenessInfo.getTimestamp();

            final Optional<ExecutedActionsChangeWindow> oldChangeWindow = Optional.ofNullable(
                    cacheByActionId.get(actionId));
            if (!oldChangeWindow.isPresent()) {
                // Tried to update an action in cache, but there was no entry for it. It should not
                // happen as we already checked that before adding to the staging list.
                logger.warn("Missing action liveness info {} in cache.",
                        SavingsUtil.toString(livenessInfo, clock));
                continue;
            }

            // Once the DB upgrade is successful, we update the local cache as well if LIVE/NEW.
            // Otherwise, for non-LIVE/NEW actions, we remove cache entry, as we don't care
            // about those actions anymore. We are only tracking LIVE and NEW actions here.
            if (!trackedLivenessStates.contains(state)) {
                cacheByActionId.remove(actionId);
                logger.info("Removed old action change window {}, new liveness info: {}.",
                        SavingsUtil.toString(oldChangeWindow.get(), clock),
                        SavingsUtil.toString(livenessInfo, clock));
            } else {
                final ExecutedActionsChangeWindow.Builder newChangeWindowBuilder =
                        ExecutedActionsChangeWindow.newBuilder(oldChangeWindow.get())
                                .setLivenessState(state);
                newChangeWindowBuilder.setStartTime(timestamp);
                cacheByActionId.put(actionId, newChangeWindowBuilder.build());
                logger.info("Updated old action change window {}, new liveness info: {}.",
                        SavingsUtil.toString(oldChangeWindow.get(), clock),
                        SavingsUtil.toString(livenessInfo, clock));
            }
        }
        stagingChanges.clear();
    }

    /**
     * Adds action to staging changes list, so that they can be updated on saveChanges().
     *
     * @param actionId Action id.
     * @param timestamp Timestamp (start/end) to use.
     * @param state Liveness state.
     */
    private void updateAction(long actionId, long timestamp, @Nonnull LivenessState state) {
        stagingChanges.add(ActionLivenessInfo.newBuilder().setActionOid(actionId).setTimestamp(
                timestamp).setLivenessState(state).build());
    }
}
