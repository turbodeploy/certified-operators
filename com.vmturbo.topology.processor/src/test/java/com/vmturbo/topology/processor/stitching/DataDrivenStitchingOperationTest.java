package com.vmturbo.topology.processor.stitching;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommoditySoldMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MergePropertiesStrategy;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.StitchingScope;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.StitchingScopeType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.AbstractExternalSignatureCachingStitchingOperation;
import com.vmturbo.stitching.DTOFieldSpec;
import com.vmturbo.stitching.DataDrivenStitchingOperation;
import com.vmturbo.stitching.MatchingPropertyOrField;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingMatchingMetaData;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.MergeEntities.MergeEntitiesDetails;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Test the basic functionality of the DataDrivenStitchingOperation class around setting
 * setting isCachingEnabled properly.
 */
public class DataDrivenStitchingOperationTest {

    @Mock
    private StitchingChangesBuilder<StitchingEntity> resultBuilder1;

    @Mock
    private StitchingChangesBuilder<StitchingEntity> resultBuilder2;

    /**
     * Rule to initialize FeatureFlags store.
     **/
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule();

    private AbstractExternalSignatureCachingStitchingOperation createStitchingOperation(boolean useParentScope,
            @Nonnull Set<ProbeCategory> scope, @Nonnull ProbeCategory category) {
        return new DataDrivenStitchingOperation(new TestStitchingMatchingMetadata(useParentScope),
                scope, category, MergePropertiesStrategy.MERGE_NOTHING);
    }

    /**
     * Test that caching is enabled when there is no parent scope and there is a list of
     * categories that does not overlap with the probe category of the operation.
     */
    @Test
    public void testCachingEnabled() {
        final AbstractExternalSignatureCachingStitchingOperation operation =
                createStitchingOperation(false,
                Collections.singleton(ProbeCategory.HYPERVISOR),
                ProbeCategory.VIRTUAL_DESKTOP_INFRASTRUCTURE);
        assertTrue(operation.isCachingEnabled());
    }

    /**
     * Test that caching is disabled for when the matching metadata has a parent scope defined.
     */
    @Test
    public void testCachingDisabledForParentScope() {
        final AbstractExternalSignatureCachingStitchingOperation operation =
                createStitchingOperation(true,
                        Collections.singleton(ProbeCategory.HYPERVISOR),
                        ProbeCategory.VIRTUAL_DESKTOP_INFRASTRUCTURE);
        assertFalse(operation.isCachingEnabled());
    }

    /**
     * Test that if the operation is stitching with all categories, we disable caching.
     */
    @Test
    public void testCachingDisabledForGlobalScope() {
        final AbstractExternalSignatureCachingStitchingOperation operation =
                createStitchingOperation(false,
                        Collections.emptySet(),
                        ProbeCategory.VIRTUAL_DESKTOP_INFRASTRUCTURE);
        assertFalse(operation.isCachingEnabled());
    }

    /**
     * Test that when the stitching scope includes the ProbeCategory of the operation itself,
     * caching is disabled.
     */
    @Test
    public void testCachingDisabledWhenScopeIncludesCategory() {
        final AbstractExternalSignatureCachingStitchingOperation operation =
                createStitchingOperation(false,
                        ImmutableSet.of(ProbeCategory.HYPERVISOR,
                                ProbeCategory.VIRTUAL_DESKTOP_INFRASTRUCTURE),
                        ProbeCategory.VIRTUAL_DESKTOP_INFRASTRUCTURE);
        assertFalse(operation.isCachingEnabled());
    }

    /**
     * Test that when we have multiple potential external entities to stitch with, we always
     * stitch with the one with the smallest OID.
     */
    @Test
    public void testStitchWithSameExternalEntityRegardlessOfOrder() {
        MockitoAnnotations.initMocks(this);
        final long internalEntity1Oid = 1234L;
        final long internalEntity2Oid = 4567L;
        final long externalEntity1Oid = 2345L;
        final long externalEntity2Oid = 3456L;
        final StitchingEntity internalEntity1 = Mockito.mock(StitchingEntity.class);
        final StitchingEntity internalEntity2 = Mockito.mock(StitchingEntity.class);
        final StitchingEntity externalEntity1 = Mockito.mock(StitchingEntity.class);
        final StitchingEntity externalEntity2 = Mockito.mock(StitchingEntity.class);
        final String internalEntity1DisplayName = "internalEntity1";
        final String internalEntity2DisplayName = "internalEntity2";
        final String externalEntity1DisplayName = "externalEntity1";
        final String externalEntity2DisplayName = "externalEntity2";

        Mockito.when(internalEntity1.getOid()).thenReturn(internalEntity1Oid);
        Mockito.when(internalEntity2.getOid()).thenReturn(internalEntity2Oid);
        Mockito.when(externalEntity1.getOid()).thenReturn(externalEntity1Oid);
        Mockito.when(externalEntity2.getOid()).thenReturn(externalEntity2Oid);
        Mockito.when(internalEntity1.getDisplayName()).thenReturn(internalEntity1DisplayName);
        Mockito.when(internalEntity2.getDisplayName()).thenReturn(internalEntity2DisplayName);
        Mockito.when(externalEntity1.getDisplayName()).thenReturn(externalEntity1DisplayName);
        Mockito.when(externalEntity2.getDisplayName()).thenReturn(externalEntity2DisplayName);
        final Collection<StitchingPoint> stitchingPoints1 = Lists.newArrayList();
        final Collection<StitchingPoint> stitchingPoints2 = Lists.newArrayList();
        stitchingPoints1.add(new StitchingPoint(internalEntity1, Lists.newArrayList(externalEntity1,
                externalEntity2)));
        stitchingPoints1.add(new StitchingPoint(internalEntity2, Lists.newArrayList(externalEntity1,
                externalEntity2)));
        stitchingPoints2.add(new StitchingPoint(internalEntity1, Lists.newArrayList(externalEntity2,
                externalEntity1)));
        final AbstractExternalSignatureCachingStitchingOperation operation =
                createStitchingOperation(false,
                        Collections.singleton(ProbeCategory.HYPERVISOR),
                        ProbeCategory.VIRTUAL_DESKTOP_INFRASTRUCTURE);
        final ArgumentCaptor<MergeEntitiesDetails> mergeEntitiesDetails1 =
                ArgumentCaptor.forClass(MergeEntitiesDetails.class);
        final ArgumentCaptor<MergeEntitiesDetails> mergeEntitiesDetails2 =
                ArgumentCaptor.forClass(MergeEntitiesDetails.class);
        operation.stitch(stitchingPoints1, resultBuilder1);
        operation.stitch(stitchingPoints2, resultBuilder2);
        Mockito.verify(resultBuilder1, Mockito.times(2))
                .queueEntityMerger(mergeEntitiesDetails1.capture());
        Mockito.verify(resultBuilder2).queueEntityMerger(mergeEntitiesDetails2.capture());
        final List<MergeEntitiesDetails> resultBuilder1Results =
                mergeEntitiesDetails1.getAllValues();
        Assert.assertSame(externalEntity1, resultBuilder1Results.get(0)
                .getMergeOntoEntity());
        Assert.assertSame(externalEntity1, resultBuilder1Results.get(1)
                .getMergeOntoEntity());
        Assert.assertSame(externalEntity1, mergeEntitiesDetails2.getValue().getMergeOntoEntity());
    }

    /**
     * Helper class that allows easy creation of StitchingMatchingMetadata with or without
     * Parent scope.
     */
    public static class TestStitchingMatchingMetadata implements
            StitchingMatchingMetaData<String, String> {

        private final boolean useParentScope;

        /**
         * Create and instance of the class.
         *
         * @param useParentScope whether or not to have a Parent scope associated with this
         * metadata.
         */
        public TestStitchingMatchingMetadata(boolean useParentScope) {
            this.useParentScope = useParentScope;
        }

        @Override
        public EntityType getInternalEntityType() {
            return EntityType.VIRTUAL_MACHINE;
        }

        @Nonnull
        @Override
        public Collection<MatchingPropertyOrField<String>> getInternalMatchingData() {
            return null;
        }

        @Override
        public EntityType getExternalEntityType() {
            return EntityType.VIRTUAL_MACHINE;
        }

        @Nonnull
        @Override
        public Collection<MatchingPropertyOrField<String>> getExternalMatchingData() {
            return Collections.emptyList();
        }

        @Override
        public Collection<String> getPropertiesToPatch() {
            return Collections.emptyList();
        }

        @Override
        public Collection<DTOFieldSpec> getAttributesToPatch() {
            return Collections.emptyList();
        }

        @Override
        public Collection<CommoditySoldMetadata> getCommoditiesSoldToPatch() {
            return Collections.emptyList();
        }

        @Override
        public Collection<CommodityBoughtMetadata> getCommoditiesBoughtToPatch() {
            return Collections.emptyList();
        }

        @Override
        public boolean getKeepStandalone() {
            return false;
        }

        @Nonnull
        @Override
        public Optional<StitchingScope> getStitchingScope() {
            return useParentScope ? Optional.of(StitchingScope.newBuilder().setScopeType(
                    StitchingScopeType.PARENT).build()) : Optional.empty();
        }
    }
}
