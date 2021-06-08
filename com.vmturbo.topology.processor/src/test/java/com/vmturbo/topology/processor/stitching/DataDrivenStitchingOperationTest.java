package com.vmturbo.topology.processor.stitching;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommoditySoldMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.StitchingScope;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.StitchingScopeType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.AbstractExternalSignatureCachingStitchingOperation;
import com.vmturbo.stitching.DTOFieldSpec;
import com.vmturbo.stitching.DataDrivenStitchingOperation;
import com.vmturbo.stitching.MatchingPropertyOrField;
import com.vmturbo.stitching.StitchingMatchingMetaData;

/**
 * Test the basic functionality of the DataDrivenStitchingOperation class around setting
 * setting isCachingEnabled properly.
 */
public class DataDrivenStitchingOperationTest {

    private AbstractExternalSignatureCachingStitchingOperation createStitchingOperation(boolean useParentScope,
            @Nonnull Set<ProbeCategory> scope, @Nonnull ProbeCategory category) {
        return new DataDrivenStitchingOperation(new TestStitchingMatchingMetadata(useParentScope),
                scope, category);
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
