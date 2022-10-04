package com.vmturbo.cloud.common.scope;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.scope.CloudScopeIdentity.CloudScopeType;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityStore.CloudScopeIdentityFilter;

/**
 * A {@link CloudScopeIdentityProvider} in which aggregate scope identities are cached to facilitate efficient assignment
 * of scope IDs for aggregate scopes, without requiring querying the underlying {@link CloudScopeIdentityStore} to determine
 * previously assigned scoped IDs.
 *
 * <p>For resource scopes, this store will assign a scope ID matching the resource ID. For aggregate scopes (e.g. the
 * intersection of an account and region), an ID will be assigned. While this store makes a best-effort attempt to
 * consistently assign the same scope ID to an aggregate scope, this is not guaranteed. It is assumed having multiple
 * scope IDs for the same aggregate scope, while inefficient, will not break any requirements for data referencing
 * the cloud scope.
 */
public class CachedAggregateScopeIdentityProvider implements CloudScopeIdentityProvider {

    private final Logger logger = LogManager.getLogger();

    private final CloudScopeIdentityStore persistenceStore;

    private final IdentityProvider identityProvider;

    private final ReadWriteLock initializationLock = new ReentrantReadWriteLock();

    @GuardedBy("initializationLock")
    private boolean initialized = false;

    private final Map<CloudScope, CloudScopeIdentity> scopeIdentityCache = new ConcurrentHashMap<>();

    private final CacheInitializer cacheInitializer;

    /**
     * Constructs the identity provider.
     * @param persistenceStore The cloud scope identity persistence store.
     * @param identityProvider The base identity provider, used to generate IDs for cloud scopes.
     * @param initializationTimeout The timeout to wait for cache initialization
     */
    public CachedAggregateScopeIdentityProvider(@Nonnull CloudScopeIdentityStore persistenceStore,
                                                @Nonnull IdentityProvider identityProvider,
                                                @Nonnull Duration initializationTimeout) {

        this.persistenceStore = Objects.requireNonNull(persistenceStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.cacheInitializer = new CacheInitializer(initializationTimeout);
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public Map<CloudScope, CloudScopeIdentity> getOrCreateScopeIdentities(@Nonnull Collection<CloudScope> cloudScopes)
            throws IdentityOperationException, IdentityUninitializedException {

        logger.debug("Initiating cloud scope identity creation for {} cloud scopes", cloudScopes::size);

        initializationLock.readLock().lock();
        try {

            final Stopwatch stopwatch = Stopwatch.createStarted();
            checkInitialized();

            final Map<CloudScope, CloudScopeIdentity> scopeIdentityMap = cloudScopes.stream()
                    .collect(ImmutableMap.toImmutableMap(
                            Function.identity(),
                            this::getOrCreateIdentity));

            final IdentityStoreOperation storeOperation = new IdentityStoreOperation(scopeIdentityMap.values());
            storeOperation.run();

            logger.info("Resolved {} cloud scopes in {}", cloudScopes.size(), stopwatch);

            return scopeIdentityMap;

        } catch (IdentityOperationException | IdentityUninitializedException e) {

            // If there was an exception during the store operation or the cache is currently
            // uninitialized, schedule initialization. If an initialization operation is already
            // running, this will be a no-op
            cacheInitializer.runAsync();

            // Rethrow the exception to block any current operations with potentially invalid
            // scope data
            throw e;
        } finally {
            initializationLock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void initialize() throws InitializationException {
        cacheInitializer.run();
    }

    private boolean invalidateCache() {

        initializationLock.writeLock().lock();
        try {

            boolean invalidated = initialized;
            initialized = false;
            scopeIdentityCache.clear();

            return invalidated;
        } finally {
            initializationLock.writeLock().unlock();
        }
    }

    /**
     * Initializes the scope cache with stored aggregate scope identities from the persistence store.
     */
    public void initializeCache() {

        initializationLock.writeLock().lock();
        try {

            final Stopwatch stopwatch = Stopwatch.createStarted();
            invalidateCache();

            final CloudScopeIdentityFilter aggregateIdentityFilter = CloudScopeIdentityFilter.builder()
                    .addScopeType(CloudScopeType.AGGREGATE)
                    .build();

            final MutableLong duplicateCount = new MutableLong(0);
            persistenceStore.getIdentitiesByFilter(aggregateIdentityFilter).forEach(scopeIdentity ->
                    // Store the scope identity. If an identity is already assigned for a given scope, choose
                    // the identity with the lower scope ID.
                    scopeIdentityCache.compute(scopeIdentity.toCloudScope(), (scope, previousIdentity) -> {
                        if (previousIdentity == null) {
                            return scopeIdentity;
                        } else {
                            duplicateCount.increment();
                            return scopeIdentity.scopeId() < previousIdentity.scopeId() ? scopeIdentity : previousIdentity;
                        }
                    }));

            logger.info("Cache initialization of {} aggregate scopes (with {} duplicates) took {}",
                    scopeIdentityCache.size(), duplicateCount.getValue(), stopwatch);

            initialized = true;
        } finally {
            initializationLock.writeLock().unlock();
        }
    }

    private CloudScopeIdentity getOrCreateIdentity(@Nonnull CloudScope cloudScope) {

        final CloudScopeIdentity scopeIdentity;

        if (cloudScope.hasResourceInfo()) {
            scopeIdentity = CloudScopeIdentity.builder()
                    .scopeId(cloudScope.resourceId())
                    .scopeType(CloudScopeType.RESOURCE)
                    .populateFromCloudScope(cloudScope)
                    .build();
        } else {
            scopeIdentity = scopeIdentityCache.computeIfAbsent(cloudScope, cs ->
                    CloudScopeIdentity.builder()
                            .scopeId(identityProvider.next())
                            .scopeType(CloudScopeType.AGGREGATE)
                            .populateFromCloudScope(cs)
                            .build());
        }

        return scopeIdentity;
    }

    private void checkInitialized() throws IdentityUninitializedException {

        initializationLock.readLock().lock();
        try {
            if (!initialized) {
                throw new IdentityUninitializedException();
            }
        } finally {
            initializationLock.readLock().unlock();
        }
    }

    /**
     * An operation to store the configured scope identities in the persistence store.
     */
    private class IdentityStoreOperation {

        private final List<CloudScopeIdentity> cloudScopeIdentities;

        IdentityStoreOperation(@Nonnull Collection<CloudScopeIdentity> scopeIdentities) {
            this.cloudScopeIdentities = ImmutableList.copyOf(scopeIdentities);
        }

        public synchronized void run() throws IdentityOperationException {

            try {
                persistenceStore.saveScopeIdentities(cloudScopeIdentities);
            } catch (Exception e) {
                throw new IdentityOperationException(e);
            }
        }
    }

    /**
     * Class responsible for initializing the identity cache of {@link CachedAggregateScopeIdentityProvider}, either
     * synchronously or asynchronously.
     */
    private class CacheInitializer {

        private final Duration initializationTimeout;

        private final AtomicBoolean isRunning = new AtomicBoolean(false);

        CacheInitializer(@Nonnull Duration initializationTimeout) {
            this.initializationTimeout = Objects.requireNonNull(initializationTimeout);
        }

        public void run() {

            if (!isRunning.get()) {
                runInternal();
            }
        }

        public void runAsync() {

            // Don't schedule if a concurrent initialization is already in progress
            if (!isRunning.get()) {

                logger.info("Running async cache initialization");

                final ExecutorService executor = Executors.newSingleThreadExecutor();

                CompletableFuture.runAsync(this::runInternal, executor).whenComplete((__, throwable) -> {

                    if (throwable != null) {
                        logger.error("Cache initialization threw an exception", throwable);
                    }

                    // This will cancel the thread this is running in
                    executor.shutdownNow();
                });
            }
        }

        private void runInternal() {

            try {

                if (isRunning.compareAndSet(false, true)) {

                    final ExecutorService executor = Executors.newSingleThreadExecutor();
                    try (Closeable executorShutdown = executor::shutdownNow) {
                        final Future<?> initializationFuture = executor.submit(CachedAggregateScopeIdentityProvider.this::initializeCache);

                        if (!initializationTimeout.isNegative() && !initializationTimeout.isZero()) {
                            initializationFuture.get(initializationTimeout.toNanos(), TimeUnit.NANOSECONDS);
                        } else {
                            // wait indefinitely
                            initializationFuture.get();
                        }
                    }
                }

            } catch (InterruptedException e) {
                logger.error("Interrupted initializing cloud scope cache!");
            } catch (ExecutionException | IOException e) {
                logger.error("Exception initializing cloud scope cache!", e);
            } catch (TimeoutException e) {
                logger.error("Timed out initializing cloud scope cache!");
            } finally {
                isRunning.set(false);
            }
        }
    }
}
