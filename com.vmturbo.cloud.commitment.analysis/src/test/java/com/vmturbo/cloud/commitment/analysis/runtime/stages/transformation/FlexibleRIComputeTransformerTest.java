package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate.DemandTimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.FlexibleRIComputeTransformer.FlexibleRIComputeTransformerFactory;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierNotFoundException;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.IncompatibleTiersException;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class FlexibleRIComputeTransformerTest {


    final DemandTimeSeries secondaryTimeSeries = DemandTimeSeries.builder()
            .cloudTierDemand(ComputeTierDemand.builder()
                    .cloudTierOid(2)
                    .osType(OSType.RHEL)
                    .tenancy(Tenancy.DEFAULT)
                    .build())
            .addDemandIntervals(TimeInterval.builder()
                    .startTime(Instant.ofEpochSecond(Duration.ofHours(2).getSeconds()))
                    .endTime(Instant.ofEpochSecond(Duration.ofHours(3).getSeconds()))
                    .build())
            .build();

    final DemandTimeSeries allocatedTimeSeries = DemandTimeSeries.builder()
            .cloudTierDemand(ComputeTierDemand.builder()
                    .cloudTierOid(1)
                    .osType(OSType.RHEL)
                    .tenancy(Tenancy.DEFAULT)
                    .build())
            .addDemandIntervals(TimeInterval.builder()
                    .startTime(Instant.ofEpochSecond(Duration.ofHours(3).getSeconds()))
                    .endTime(Instant.ofEpochSecond(Duration.ofHours(4).getSeconds()))
                    .build())
            .build();
    private final ClassifiedEntityDemandAggregate entityAggregate = ClassifiedEntityDemandAggregate.builder()
            .entityOid(1)
            .accountOid(2)
            .regionOid(3)
            .serviceProviderOid(4)
            .putClassifiedCloudTierDemand(
                    DemandClassification.of(AllocatedDemandClassification.ALLOCATED),
                    Collections.singleton(allocatedTimeSeries))
            .allocatedCloudTierDemand(allocatedTimeSeries)
            .isTerminated(true)
            .isSuspended(true)
            .build();

    final ComputeTierFamilyResolver computeTierFamilyResolver = mock(ComputeTierFamilyResolver.class);

    private final FlexibleRIComputeTransformerFactory transformerFactory =
            new FlexibleRIComputeTransformerFactory();

    private DemandTransformationJournal transformationJournal;

    @Before
    public void setup() {
        transformationJournal = DemandTransformationJournal.newJournal();
    }

    @Test
    public void testEntityWithoutAllocatedDemand() {
        final ClassifiedEntityDemandAggregate modifiedAggregate = ClassifiedEntityDemandAggregate.builder()
                .from(entityAggregate)
                .allocatedCloudTierDemand(Optional.empty())
                .build();

        // invoke transformer
        final FlexibleRIComputeTransformer transformer = transformerFactory.newTransformer(
                transformationJournal, computeTierFamilyResolver);
        final ClassifiedEntityDemandAggregate transformedAggregate = transformer.transformDemand(modifiedAggregate);

        // assert no change to the demand
        assertThat(transformedAggregate, equalTo(modifiedAggregate));
    }

    @Test
    public void testSmallerFlexibleDemand() throws IncompatibleTiersException, ComputeTierNotFoundException {

        // add flexible demand
        final ClassifiedEntityDemandAggregate flexibleAggregate = ClassifiedEntityDemandAggregate.builder()
                .from(entityAggregate)
                .putClassifiedCloudTierDemand(
                        DemandClassification.of(AllocatedDemandClassification.FLEXIBLY_ALLOCATED),
                        Collections.singleton(secondaryTimeSeries))
                .build();

        // setup the computeTierFamilyResolver
        when(computeTierFamilyResolver.compareTiers(
                eq(secondaryTimeSeries.cloudTierDemand().cloudTierOid()),
                eq(allocatedTimeSeries.cloudTierDemand().cloudTierOid()))).thenReturn(-1L);

        // invoke transformer
        final FlexibleRIComputeTransformer transformer = transformerFactory.newTransformer(
                transformationJournal, computeTierFamilyResolver);
        final ClassifiedEntityDemandAggregate transformedAggregate = transformer.transformDemand(flexibleAggregate);

        // assert no change to flexible demand
        assertThat(transformedAggregate.classifiedCloudTierDemand(), equalTo(flexibleAggregate.classifiedCloudTierDemand()));
    }

    @Test
    public void testLargerFlexibleDemand() throws IncompatibleTiersException, ComputeTierNotFoundException {

        // add flexible demand
        final ClassifiedEntityDemandAggregate flexibleAggregate = ClassifiedEntityDemandAggregate.builder()
                .from(entityAggregate)
                .putClassifiedCloudTierDemand(
                        DemandClassification.of(AllocatedDemandClassification.FLEXIBLY_ALLOCATED),
                        Collections.singleton(secondaryTimeSeries))
                .build();

        // setup the computeTierFamilyResolver
        when(computeTierFamilyResolver.compareTiers(
                eq(secondaryTimeSeries.cloudTierDemand().cloudTierOid()),
                eq(allocatedTimeSeries.cloudTierDemand().cloudTierOid()))).thenReturn(1L);

        // invoke transformer
        final FlexibleRIComputeTransformer transformer = transformerFactory.newTransformer(
                transformationJournal, computeTierFamilyResolver);
        final ClassifiedEntityDemandAggregate transformedAggregate = transformer.transformDemand(flexibleAggregate);

        // Assertions
        final ClassifiedEntityDemandAggregate expectedAggregate = ClassifiedEntityDemandAggregate.builder()
                .from(entityAggregate)
                .putClassifiedCloudTierDemand(
                        DemandClassification.of(AllocatedDemandClassification.FLEXIBLY_ALLOCATED),
                        Collections.singleton(DemandTimeSeries.builder()
                                .from(secondaryTimeSeries)
                                .cloudTierDemand(allocatedTimeSeries.cloudTierDemand())
                                .build()))
                .build();

        assertThat(transformedAggregate, equalTo(expectedAggregate));
    }

    /**
     * Tests a VM in which it has two prior allocations on other instance sizes within the same family
     * as the current allocation.
     */
    @Test
    public void testMultipleFlexibleDemand() throws IncompatibleTiersException, ComputeTierNotFoundException {

        final DemandTimeSeries thirdTimeSeries = DemandTimeSeries.builder()
                .cloudTierDemand(ComputeTierDemand.builder()
                        .cloudTierOid(3)
                        .osType(OSType.RHEL)
                        .tenancy(Tenancy.DEFAULT)
                        .build())
                .addDemandIntervals(TimeInterval.builder()
                        .startTime(Instant.ofEpochSecond(Duration.ofHours(1).getSeconds()))
                        .endTime(Instant.ofEpochSecond(Duration.ofHours(2).getSeconds()))
                        .build())
                .build();

        // add flexible demand
        final ClassifiedEntityDemandAggregate flexibleAggregate = ClassifiedEntityDemandAggregate.builder()
                .from(entityAggregate)
                .putClassifiedCloudTierDemand(
                        DemandClassification.of(AllocatedDemandClassification.FLEXIBLY_ALLOCATED),
                        ImmutableSet.of(secondaryTimeSeries, thirdTimeSeries))
                .build();

        // setup the computeTierFamilyResolver
        when(computeTierFamilyResolver.compareTiers(
                eq(secondaryTimeSeries.cloudTierDemand().cloudTierOid()),
                eq(allocatedTimeSeries.cloudTierDemand().cloudTierOid()))).thenReturn(1L);

        when(computeTierFamilyResolver.compareTiers(
                eq(thirdTimeSeries.cloudTierDemand().cloudTierOid()),
                eq(allocatedTimeSeries.cloudTierDemand().cloudTierOid()))).thenReturn(1L);

        // invoke transformer
        final FlexibleRIComputeTransformer transformer = transformerFactory.newTransformer(
                transformationJournal, computeTierFamilyResolver);
        final ClassifiedEntityDemandAggregate transformedAggregate = transformer.transformDemand(flexibleAggregate);

        // Assertions
        final ClassifiedEntityDemandAggregate expectedAggregate = ClassifiedEntityDemandAggregate.builder()
                .from(entityAggregate)
                .putClassifiedCloudTierDemand(
                        DemandClassification.of(AllocatedDemandClassification.FLEXIBLY_ALLOCATED),
                        ImmutableSet.of(
                                DemandTimeSeries.builder()
                                        .addAllDemandIntervals(secondaryTimeSeries.demandIntervals())
                                        .cloudTierDemand(allocatedTimeSeries.cloudTierDemand())
                                        .build(),
                                DemandTimeSeries.builder()
                                        .addAllDemandIntervals(thirdTimeSeries.demandIntervals())
                                        .cloudTierDemand(allocatedTimeSeries.cloudTierDemand())
                                        .build()))
                .build();

        assertThat(transformedAggregate, equalTo(expectedAggregate));
    }
}
