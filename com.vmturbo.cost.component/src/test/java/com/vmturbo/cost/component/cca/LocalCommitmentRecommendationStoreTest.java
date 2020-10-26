package com.vmturbo.cost.component.cca;

import static com.vmturbo.cost.component.cca.CloudCommitmentRecommendationTest.DEMAND_SEGMENT_2;
import static com.vmturbo.cost.component.cca.CloudCommitmentRecommendationTest.RESERVED_INSTANCE_RECOMMENDATION;
import static com.vmturbo.cost.component.cca.CloudCommitmentRecommendationTest.RI_SAVINGS_CALCULATION_RECOMMENDATION;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.junit.Before;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.TopologyReference;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore.DemandType;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore.RIBuyInstanceDemand;
import com.vmturbo.cost.component.reserved.instance.BuyReservedInstanceStore;

public class LocalCommitmentRecommendationStoreTest {

    private final BuyReservedInstanceStore buyReservedInstanceStore = mock(BuyReservedInstanceStore.class);

    private final ActionContextRIBuyStore actionContextRIBuyStore = mock(ActionContextRIBuyStore.class);

    private final MinimalCloudTopology<MinimalEntity> cloudTopology = mock(MinimalCloudTopology.class);

    private LocalCommitmentRecommendationStore recommendationStore;

    @Before
    public void testReservedInstancePersistence() {

        // setup the cloud topology
        when(cloudTopology.entityExists(anyLong())).thenReturn(true);

        final CloudCommitmentAnalysisInfo analysisInfo = CloudCommitmentAnalysisInfo.newBuilder()
                .setOid(2L)
                .setCreationTime(Instant.now().toEpochMilli())
                .setAnalysisTopology(TopologyReference.newBuilder()
                        .setTopologyContextId(123L)
                        .build())
                .build();

        // invoke the store
        recommendationStore.persistRecommendations(
                analysisInfo,
                ImmutableList.of(RESERVED_INSTANCE_RECOMMENDATION),
                cloudTopology);


        // check the buy RI store
        final ArgumentCaptor<Set> riDataSetCaptor = ArgumentCaptor.forClass(Set.class);
        final ArgumentCaptor<Long> topologyContextIDCaptor = ArgumentCaptor.forClass(Long.class);
        verify(buyReservedInstanceStore).updateBuyReservedInstances(
                riDataSetCaptor.capture(),
                topologyContextIDCaptor.capture());

        final Set<ReservedInstanceData> riDataSet = riDataSetCaptor.getValue();
        assertThat(riDataSet, hasSize(1));
        final ReservedInstanceData riData = Iterables.getOnlyElement(riDataSet);
        final ReservedInstanceBoughtInfo riBoughtInfo = riData.commitment().getReservedInstanceBoughtInfo();
        assertThat(riBoughtInfo.getBusinessAccountId(),
                equalTo(RESERVED_INSTANCE_RECOMMENDATION.recommendationInfo().purchasingAccountOid()));
        assertThat(riBoughtInfo.getNumBought(), equalTo(RI_SAVINGS_CALCULATION_RECOMMENDATION.recommendationQuantity()));

        // check the RI buy demand context store
        final ArgumentCaptor<List> instanceDemandListCaptor = ArgumentCaptor.forClass(List.class);
        verify(actionContextRIBuyStore).insertRIBuyInstanceDemand(instanceDemandListCaptor.capture());

        final List<RIBuyInstanceDemand> instanceDemandList = instanceDemandListCaptor.getValue();
        assertThat(instanceDemandList, hasSize(1));
        final RIBuyInstanceDemand instanceDemand = instanceDemandList.get(0);
        assertThat(instanceDemand.actionId(), equalTo(RESERVED_INSTANCE_RECOMMENDATION.recommendationId()));
        assertThat(instanceDemand.topologyContextId(), equalTo(analysisInfo.getAnalysisTopology().getTopologyContextId()));
        assertThat(instanceDemand.demandType(), equalTo(DemandType.OBSERVED_DEMAND));
        assertThat(instanceDemand.lastDatapointTime(), equalTo(DEMAND_SEGMENT_2.timeInterval().startTime()));
        assertThat(instanceDemand.datapoints(), hasSize(2));
    }
}
