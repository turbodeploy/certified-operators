package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.platform.common.builders.EntityBuilders.storage;
import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.AbstractExternalSignatureCachingStitchingOperation;
import com.vmturbo.stitching.ExternalSignatureCache;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Tests of AbstractExternalSignatureCachingStitchingOperation's caching functionality.
 */
public class AbstractExternalSignatureCachingStitchingOperationTest {

    private final TargetStore targetStore = mock(TargetStore.class);

    @Mock
    private StitchingScopeFactory<StitchingEntity> scopeFactory;

    private StitchingScope<StitchingEntity> globalScope = new StitchingScope<StitchingEntity>() {
        @Nonnull
        @Override
        public Stream<StitchingEntity> entities() {
            return stitchingContext.getStitchingGraph().entities()
                    .map(StitchingEntity.class::cast);
        }
    };

    private StitchingScope<StitchingEntity> vmScope = new StitchingScope<StitchingEntity>() {
        @Nonnull
        @Override
        public Stream<StitchingEntity> entities() {
            return stitchingContext.getStitchingGraph().entities()
                    .filter(entity -> entity.getEntityBuilder().getEntityType()
                            == EntityType.VIRTUAL_MACHINE)
                    .map(StitchingEntity.class::cast);
        }
    };

    private StitchingContext stitchingContext;

    private final long targetId = 5678L;

    private final long unusedTargetId = 6789L;

    private final EntityDTO.Builder vmFoo = virtualMachine("foo")
            .guestName("foo")
            .build().toBuilder();

    private final EntityDTO.Builder vmBar = virtualMachine("bar")
            .guestName("bar")
            .build().toBuilder();

    private final EntityDTO.Builder storageOne = storage("one")
            .build().toBuilder();

    private final Map<String, StitchingEntityData> entityData =
            ImmutableMap.<String, StitchingEntityData>builder()
                    .put(vmFoo.getId(), nextEntity(vmFoo, targetId))
                    .put(vmBar.getId(), nextEntity(vmBar, targetId))
                    .put(storageOne.getId(), nextEntity(storageOne, targetId))
            .build();

    private long curOid = 1L;

    private StitchingEntityData nextEntity(@Nonnull final EntityDTO.Builder entityDto,
            final long targetId) {
        return StitchingEntityData.newBuilder(entityDto)
                .targetId(targetId)
                .oid(curOid++)
                .lastUpdatedTime(0)
                .build();
    }

    /**
     * Setup the stitching context and the mock for the scope factory.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        final StitchingContext.Builder contextBuilder = StitchingContext.newBuilder(5,
                targetStore)
                .setIdentityProvider(mock(IdentityProviderImpl.class));
        entityData.values()
                .forEach(entity -> contextBuilder.addEntity(entity, entityData));
        stitchingContext = contextBuilder.build();
        when(scopeFactory.globalScope()).thenReturn(globalScope);
        when(scopeFactory.entityTypeScope(eq(EntityType.VIRTUAL_MACHINE))).thenReturn(vmScope);
    }

    private void callGetExternalSignaturesTwice(int numberOfScopeFactoryCallsExpected,
            @Nonnull StitchingOperation<String, String> operation) {
        final ExternalSignatureCache cache = new ExternalSignatureCache();
        // call getExternalSignatures twice without calling initializeOperationBeforeStitching
        // in between and confirm that we only called the scope factory once
        assertThat(operation.getExternalSignatures(scopeFactory, cache, targetId).keySet(),
                containsInAnyOrder("foo", "bar", "one"));
        assertThat(operation.getExternalSignatures(scopeFactory, cache, unusedTargetId).keySet(),
                containsInAnyOrder("foo", "bar", "one"));
        verify(scopeFactory, times(numberOfScopeFactoryCallsExpected)).globalScope();
    }

    /**
     * Test that when you call a caching operation's getExternalSignatures method twice, we only
     * fetch the scope from the factory once, since we already have the signatures cached by the
     * second call.
     */
    @Test
    public void testExternalSignaturesCached() {
        final StitchingOperation<String, String> operation = new OperationWithScopeAndCaching();
        callGetExternalSignaturesTwice(1, operation);
    }

    /**
     * Test that turning off the cache works correctly.
     */
    @Test
    public void testExternalSignaturesNonCached() {
        final StitchingOperation<String, String> operation = new NonCachingOperation();
        callGetExternalSignaturesTwice(2, operation);
    }

    /**
     * Test that if an operation returns an empty scope, we get the map from the entity type scope
     * and cache that map.
     */
    @Test
    public void testScopeIsEmpty() {
        final StitchingOperation<String, String> operation = new ScopelessOperation();
        final ExternalSignatureCache signatureCache = new ExternalSignatureCache();
        final Map<String, Collection<StitchingEntity>> mapReturned =
                operation.getExternalSignatures(scopeFactory, signatureCache, targetId);
        assertEquals(2, mapReturned.size());
        assertThat(mapReturned.keySet(), containsInAnyOrder("foo", "bar"));

        // at this point, the external signature map should be cached. So if we call it again,
        // we should not get the scope again and we should just get the same map back.
       final Map<String, Collection<StitchingEntity>> secondMapReturned =
                operation.getExternalSignatures(scopeFactory, signatureCache, targetId);
        assertSame(mapReturned, secondMapReturned);

        verify(scopeFactory, times(1))
                .entityTypeScope(eq(EntityType.VIRTUAL_MACHINE));
    }

    /**
     * Ensure that the context created by an operation is passed through to its
     * {@code getExternalSignature} method.
     */
    @Test
    public void testContextPassThrough() {
        final MutableLong signatureContextHolder = new MutableLong(0);
        final StitchingOperation<String, String> operation =
            new OperationWithExternalContext(signatureContextHolder);
        final ExternalSignatureCache signatureCache = new ExternalSignatureCache();
        operation.getExternalSignatures(scopeFactory, signatureCache, targetId);

        assertEquals(OperationWithExternalContext.CONTEXT_ID, signatureContextHolder.longValue());
    }

    /**
     * Implementation of AbstractExternalSignatureCachingStitchingOperation with minimal logic
     * used to test functionality provided by abstract class.
     *
     * @param <T> The External Signature Context type.
     */
    public abstract static class AbstractOperationWithScopeAndCaching<T>
            extends AbstractExternalSignatureCachingStitchingOperation<String, String, T> {
        @Nonnull
        @Override
        public Optional<StitchingScope<StitchingEntity>> getScope(
                @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory,
                long targetId) {
            return Optional.of(stitchingScopeFactory.globalScope());
        }

        @Nonnull
        @Override
        public EntityType getInternalEntityType() {
            return EntityType.VIRTUAL_MACHINE;
        }

        @Nonnull
        @Override
        public Optional<EntityType> getExternalEntityType() {
            return Optional.of(EntityType.VIRTUAL_MACHINE);
        }

        @Override
        public Collection<String> getInternalSignature(@Nonnull StitchingEntity internalEntity) {
            return null;
        }

        @Nonnull
        @Override
        public TopologicalChangelog<StitchingEntity> stitch(
                @Nonnull Collection<StitchingPoint> stitchingPoints,
                @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
            return null;
        }
    }

    /**
     * OperationWithScopeAndCaching.
     */
    public static class OperationWithScopeAndCaching extends AbstractOperationWithScopeAndCaching<Void> {
        @Override
        protected Collection<String> getExternalSignature(@Nonnull StitchingEntity externalEntity,
                                                          @Nullable Void signatureContext) {
            return Collections.singletonList(externalEntity.getEntityBuilder().getId());
        }

        @Override
        protected Void createExternalSignatureContext(
            @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
            return null;
        }
    }

    /**
     * Test class with caching turned off.
     */
    public static class NonCachingOperation extends OperationWithScopeAndCaching {
        @Override
        public boolean isCachingEnabled() {
            return false;
        }
    }

    /**
     * Test class with caching enabled but an empty scope.
     */
    public static class ScopelessOperation extends OperationWithScopeAndCaching {
        @Nonnull
        @Override
        public Optional<StitchingScope<StitchingEntity>> getScope(
                @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory,
                long targetId) {
            return Optional.empty();
        }
    }

    /**
     * Test class that has a non-Void context.
     */
    public static class OperationWithExternalContext extends AbstractOperationWithScopeAndCaching<Long> {

        private static final long CONTEXT_ID = 1234L;
        private final MutableLong signatureContextHolder;

        private OperationWithExternalContext(@Nonnull final MutableLong signatureContextHolder) {
            this.signatureContextHolder = signatureContextHolder;
        }

        @Override
        protected Collection<String> getExternalSignature(@Nonnull StitchingEntity externalEntity,
                                                          @Nullable Long signatureContext) {
            this.signatureContextHolder.setValue(signatureContext);
            return Collections.emptyList();
        }

        @Override
        protected Long createExternalSignatureContext(
            @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
            return CONTEXT_ID;
        }
    }
}
