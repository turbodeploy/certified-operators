package com.vmturbo.auth.api.authorization;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.scoping.AccessScopeCacheKey;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.userscope.UserScope.EntityAccessScopeContents;
import com.vmturbo.common.protobuf.userscope.UserScope.EntityAccessScopeRequest;
import com.vmturbo.common.protobuf.userscope.UserScope.EntityAccessScopeResponse;
import com.vmturbo.common.protobuf.userscope.UserScope.OidSetDTO;
import com.vmturbo.common.protobuf.userscope.UserScopeServiceGrpc.UserScopeServiceBlockingStub;
import com.vmturbo.components.common.identity.OidSet;
import com.vmturbo.components.common.identity.OidSet.AllOidsSet;
import com.vmturbo.components.common.identity.RoaringBitmapOidSet;
import com.vmturbo.proactivesupport.DataMetricCounter;

/**
 * UserSessionContext is a Static Singleton object that wraps access to the current user's session
 * for information.
 *
 * When a user is scoped, this class will manage fetching the set of entity oid's in the user's
 * scope from the user scope service, as needed.
 */
public class UserSessionContext implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger();

    // Some metrics tracking how the user session context is used.
    private static final DataMetricCounter USER_SESSION_CONTEXT_REQUEST_COUNT = DataMetricCounter.builder()
            .withName("user_session_context_request_count")
            .withHelp("Total # of requests for user session context data (scoped and unscoped users).")
            .build()
            .register();

    private static final DataMetricCounter USER_SESSION_CONTEXT_CACHE_HITS = DataMetricCounter.builder()
            .withName("user_session_context_cache_hits")
            .withHelp("UserSessionContext cache hits - number of requests for scoped session data served directly from cache.")
            .build()
            .register();

    private static final DataMetricCounter USER_SESSION_CONTEXT_CACHE_CONFIRMATION_COUNT = DataMetricCounter.builder()
            .withName("user_session_context_cache_confirmation_count")
            .withHelp("UserSessionContext cache `confirms` - number of requests for scoped session data that was expired, but confirmed as still valid.")
            .build()
            .register();

    private static final DataMetricCounter USER_SESSION_CONTEXT_CACHE_FULL_FETCH_COUNT = DataMetricCounter.builder()
            .withName("user_session_context_cache_full_fetch_count")
            .withHelp("UserSessionContext cache misses - number of requests for scoped session data that required a full fetch of access scope data.")
            .build()
            .register();

    private final UserScopeServiceBlockingStub userScopeServiceClient;

    private final ScheduledExecutorService janitorService;

    private boolean cacheEnabled = true;

    // number of seconds a new / refreshed cache entry can be used before needing to be checked
    // against for staleness
    private int cacheEntryExpirationTimeSecs;

    // number of seconds between invocations of the cleanup command
    private int sessionScopeCleanupIntervalSecs;

    // when the cleanup procedure runs, it will purge any cached scope data that hasn't been
    // accessed in this many seconds.
    private int expiredScopePurgeThresholdSecs;

    private final Clock clock;

    // cache of EntityAccessScope objects. Entries have a hash value and an expiration time that
    // will be checked on retrieval, and used to determine if the entry is stale or not.
    private final Map<AccessScopeCacheKey, EntityAccessScopeCacheEntry> userAccessScopes
            = Collections.synchronizedMap(new HashMap());

    // no-args constructor is for testing convenience, also doesn't create a cleanup thread.
    @VisibleForTesting
    public UserSessionContext() {
        this.userScopeServiceClient = null;
        this.janitorService = null;
        this.clock = Clock.systemUTC(); // will update this when we add more unit tests for the cache behavior
    }

    // this version will configure a user session context with caching disabled.
    public UserSessionContext(UserScopeServiceBlockingStub userScopeServiceBlockingStub, Clock clock) {
        logger.info("Creating UserSessionContext with cache disabled.");
        this.userScopeServiceClient = userScopeServiceBlockingStub;
        this.janitorService = null;
        this.clock = clock;
        cacheEnabled = false;
    }

    // this version configures a user session context with caching.
    public UserSessionContext(UserScopeServiceBlockingStub userScopeServiceBlockingStub, Clock clock,
                              int cacheExpirationSecs, int cleanupIntervalSecs, int cachePurgeAgeSecs) {
        this.userScopeServiceClient = userScopeServiceBlockingStub;
        janitorService = Executors.newScheduledThreadPool(1);
        this.clock = clock;
        this.sessionScopeCleanupIntervalSecs = cleanupIntervalSecs;
        this.expiredScopePurgeThresholdSecs = cachePurgeAgeSecs;
        this.cacheEntryExpirationTimeSecs = cacheExpirationSecs;
        logger.info("Scheduling user session scope purges every {} secs.", sessionScopeCleanupIntervalSecs);
        janitorService.scheduleAtFixedRate(this::purgeExpiredScopes, sessionScopeCleanupIntervalSecs,
                sessionScopeCleanupIntervalSecs, TimeUnit.SECONDS);
        cacheEnabled = true;
    }

    @Override
    public void close() {
        janitorService.shutdownNow();
    }

    public boolean isUserScoped() {
        return UserScopeUtils.isUserScoped();
    }

    public boolean isUserObserver() {
        return UserScopeUtils.isUserObserver();
    }

    public EntityAccessScope getUserAccessScope() {
        USER_SESSION_CONTEXT_REQUEST_COUNT.increment();
        return getAccessScope(UserScopeUtils.getUserScopeGroups());
    }

    /**
     * Get the access scope object {@link EntityAccessScope} for the given list of scopes. This can
     * be used for plan scoping and check if the a group/entity is within plan scope.
     *
     * @param scopeIds list of oids of the scopes, which can be either entity or group
     * @return an instance of {@link EntityAccessScope}
     */
    public EntityAccessScope getAccessScope(@Nullable List<Long> scopeIds) {
        // check the fast and most common path for unscoped users.
        if (scopeIds == null || scopeIds.isEmpty()) {
            return EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE;
        }
        // if a user has a scope, use the slower caching mechanism
        return getCachedEntityAccessScope(new AccessScopeCacheKey(scopeIds, UserScopeUtils.isUserShared()));
    }

    private void purgeExpiredScopes() {
        if (userAccessScopes.size() == 0) {
            // no point in running if there are no cache entries.
            return;
        }
        logger.debug("Checking for cache entries last accessed more than {} secs ago.", expiredScopePurgeThresholdSecs);
        Instant now = clock.instant();
        int originalSize = userAccessScopes.size();
        userAccessScopes.entrySet().removeIf(entry -> {
            // remove anything that hasn't been accessed in more than expiredScopePurgeThresholdSecs ago
            boolean shouldRemove = (now.isAfter(entry.getValue().lastAccessTime.plusSeconds(expiredScopePurgeThresholdSecs)));
            return shouldRemove;
        });
        int newSize = userAccessScopes.size();
        if (newSize != originalSize) {
            logger.debug("Purged {} expired cache entries.", originalSize - newSize);
        }
    }

    /**
     * Given a list of scope groups, find the associated {@link EntityAccessScope} from cache,
     * fetching it if it doesn't exist yet, and checking for updates if it does.
     *
     * This method is synchronized to avoid concurrent fetches of the same scope data. This can be
     * improved with more granular locking.
     *
     * @param cacheKey the cache key to look up
     * @return the {@link EntityAccessScope} for the list of scope groups
     */
    private synchronized EntityAccessScope getCachedEntityAccessScope(AccessScopeCacheKey cacheKey) {
        // the user is scoped and we need to work with our local cache.
        EntityAccessScopeCacheEntry accessScopeEntry = userAccessScopes.get(cacheKey);
        EntityAccessScopeRequest.Builder requestBuilder = EntityAccessScopeRequest.newBuilder();
        if (accessScopeEntry != null) {
            // check if the cached data has expired
            Instant now = clock.instant();

            if (accessScopeEntry.lastAccessTime.plusSeconds(cacheEntryExpirationTimeSecs).isAfter(now)) {
                // if our data hasn't expired yet, use it. We don't need a new request.
                logger.debug("Reusing cached access scope data.");
                USER_SESSION_CONTEXT_CACHE_HITS.increment();
                return accessScopeEntry.entityAccessScope;
            }
            // otherwise, our data *might* have expired -- check if we need to refresh the cache by
            // passing in the current hash.
            logger.debug("Cached access scope data found, but last access was {} ms ago. Checking if still valid.",
                    Duration.between(accessScopeEntry.lastAccessTime, now).toMillis());
            requestBuilder.setCurrentScopeHash(accessScopeEntry.hash);
        }

        // use the scopes provided in AccessScopeCacheKey, which can be either user scopes or
        // more general scopes (like: plan scopes)
        requestBuilder.addAllGroupId(cacheKey.getScopeGroupOids());
        requestBuilder.setIncludeInfrastructureEntities(!cacheKey.excludesInfrastructureEntities());

        logger.trace("Sending user access scope request for {} groups.", cacheKey.getScopeGroupOids().size());
        EntityAccessScopeResponse response
                = userScopeServiceClient.getEntityAccessScopeMembers(requestBuilder.build());

        if (!response.hasEntityAccessScopeContents() && accessScopeEntry != null) {
            // scope data hasn't changed -- update the cache expiry time.
            logger.debug("Scope data still valid.");
            accessScopeEntry.updateAccessTime();
            USER_SESSION_CONTEXT_CACHE_CONFIRMATION_COUNT.increment();
            return accessScopeEntry.entityAccessScope;
        }

        logger.debug("Received fresh scope data containing {} oids.",
                response.getEntityAccessScopeContents().getAccessibleOids().hasAllOids()
                        ? "All"
                        : response.getEntityAccessScopeContents().getAccessibleOids().getArray().getOidsCount());
        // data has changed or is new -- update the cache with it.
        EntityAccessScope accessScope = toEntityAccessScope(cacheKey,
                response.getEntityAccessScopeContents());
        EntityAccessScopeCacheEntry newCacheEntry = new EntityAccessScopeCacheEntry(accessScope,
                response.getEntityAccessScopeContents().getHash());
        // if caching is disabled -- don't store the value to the cache
        if (cacheEnabled) {
            userAccessScopes.put(cacheKey, newCacheEntry);
        }
        USER_SESSION_CONTEXT_CACHE_FULL_FETCH_COUNT.increment();
        return newCacheEntry.entityAccessScope;
    }

    /**
     * Convert a {@link OidSetDTO} protobuf object to {@link OidSet} business object
     *
     * @param oidSetDTO
     * @return
     */
    @VisibleForTesting
    protected OidSet toOidSet(OidSetDTO oidSetDTO) {
        if (oidSetDTO.hasNoOids()) {
            return OidSet.EMPTY_OID_SET;
        }
        if (oidSetDTO.hasAllOids()) {
            return AllOidsSet.ALL_OIDS_SET;
        }
        return new RoaringBitmapOidSet(oidSetDTO.getArray().getOidsList());
    }

    /**
     * Convert an {@link EntityAccessScopeContents} object to a {@link EntityAccessScope}
     *
     * @param contents
     * @return
     */
    @VisibleForTesting
    protected EntityAccessScope toEntityAccessScope(AccessScopeCacheKey key, EntityAccessScopeContents contents) {
        // convert the seed oid list
        final OidSet seedOids = toOidSet(contents.getSeedOids());
        final OidSet accessibleOids = toOidSet(contents.getAccessibleOids());
        final Map<String, OidSet> oidsByEntityType = contents.getAccessibleOidsByEntityTypeMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> toOidSet(entry.getValue())));

        return new EntityAccessScope(key.getScopeGroupOids(), seedOids, accessibleOids, oidsByEntityType);
    }

    private class EntityAccessScopeCacheEntry {
        private final EntityAccessScope entityAccessScope;
        private final int hash;
        private Instant lastAccessTime;

        public EntityAccessScopeCacheEntry(EntityAccessScope scope, int hash) {
            entityAccessScope = scope;
            this.hash = hash;
            updateAccessTime();
        }

        public synchronized void updateAccessTime() {
            lastAccessTime = clock.instant();
        }
    }
}
