package com.vmturbo.cloud.commitment.analysis.runtime.stages.classification;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.CloudTierFamilyMatcher.CloudTierFamilyMatcherFactory;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecData;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecMatcher;

public class CloudTierFamilyMatcherTest {

    private final CloudTierFamilyMatcherFactory cloudTierFamilyMatcherFactory =
            new CloudTierFamilyMatcherFactory();

    private final ReservedInstanceSpecMatcher cloudCommitmentSpecMatcher =
            mock(ReservedInstanceSpecMatcher.class);

    private final ScopedCloudTierInfo demandA = mock(ScopedCloudTierInfo.class);
    private final ReservedInstanceSpecData specDataA = mock(ReservedInstanceSpecData.class);

    private final ScopedCloudTierInfo demandB = mock(ScopedCloudTierInfo.class);
    private final ReservedInstanceSpecData specDataB = mock(ReservedInstanceSpecData.class);

    @Test
    public void testSpecNotFoundForDemandA() {

        when(cloudCommitmentSpecMatcher.matchDemandToSpecs(eq(demandA))).thenReturn(Optional.empty());

        final CloudTierFamilyMatcher cloudTierFamilyMatcher =
                cloudTierFamilyMatcherFactory.newFamilyMatcher(cloudCommitmentSpecMatcher);
        final boolean matchResult = cloudTierFamilyMatcher.match(demandA, demandB);

        assertFalse(matchResult);
    }

    @Test
    public void testSpecNotFoundForDemandB() {

        when(cloudCommitmentSpecMatcher.matchDemandToSpecs(eq(demandA)))
                .thenReturn(Optional.of(specDataA));
        when(cloudCommitmentSpecMatcher.matchDemandToSpecs(eq(demandB))).thenReturn(Optional.empty());

        final CloudTierFamilyMatcher cloudTierFamilyMatcher =
                cloudTierFamilyMatcherFactory.newFamilyMatcher(cloudCommitmentSpecMatcher);
        final boolean matchResult = cloudTierFamilyMatcher.match(demandA, demandB);

        assertFalse(matchResult);
    }

    @Test
    public void testSpecsNotEqual() {

        when(cloudCommitmentSpecMatcher.matchDemandToSpecs(eq(demandA)))
                .thenReturn(Optional.of(specDataA));
        when(cloudCommitmentSpecMatcher.matchDemandToSpecs(eq(demandB)))
                .thenReturn(Optional.of(specDataB));

        final CloudTierFamilyMatcher cloudTierFamilyMatcher =
                cloudTierFamilyMatcherFactory.newFamilyMatcher(cloudCommitmentSpecMatcher);
        final boolean matchResult = cloudTierFamilyMatcher.match(demandA, demandB);

        assertFalse(matchResult);
    }

    @Test
    public void testSpecsEqual() {

        when(cloudCommitmentSpecMatcher.matchDemandToSpecs(eq(demandA)))
                .thenReturn(Optional.of(specDataA));
        when(cloudCommitmentSpecMatcher.matchDemandToSpecs(eq(demandB)))
                .thenReturn(Optional.of(specDataA));

        final CloudTierFamilyMatcher cloudTierFamilyMatcher =
                cloudTierFamilyMatcherFactory.newFamilyMatcher(cloudCommitmentSpecMatcher);
        final boolean matchResult = cloudTierFamilyMatcher.match(demandA, demandB);

        assertTrue(matchResult);
    }
}
