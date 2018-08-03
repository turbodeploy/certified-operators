package com.vmturbo.stitching.poststitching;

import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

public class ComputeQxVcpuUsedValuePostStitchingTest {

    private final static List<CommodityType> COMM_TYPES = ImmutableList.of(CommodityType.Q1_VCPU,
            CommodityType.Q2_VCPU, CommodityType.Q3_VCPU);
    private final static List<List<Double>> usedValues = ImmutableList.of(ImmutableList.of(1.0, 2.0, 3.0),
            ImmutableList.of(4.0, 4.0, 4.0),ImmutableList.of(5.0, 6.0, 7.0));
    private final static CommodityType commodityTypeExclude = CommodityType.CPU;
    private final ComputedQxVcpuUsedValuePostStitchingOperation stitchOperation =
            new ComputedQxVcpuUsedValuePostStitchingOperation();

    private final IStitchingJournal journal = mock(IStitchingJournal.class);

    final double delta = 0.0000001;


    @Test
    public void testUsedValue() {
        TopologyEntity provider = seller();

        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        stitchOperation.performOperation(
                Stream.of(provider), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        final List<CommoditySoldDTO> commoditySoldDTOList =
                provider.getTopologyEntityDtoBuilder().build().getCommoditySoldListList();
        final double q1VcpuUsed = getCommodityUsedValue(commoditySoldDTOList, COMM_TYPES.get(0).getNumber());
        final double q2VcpuUsed = getCommodityUsedValue(commoditySoldDTOList, COMM_TYPES.get(1).getNumber());
        final double q3VcpuUsed = getCommodityUsedValue(commoditySoldDTOList, COMM_TYPES.get(2).getNumber());
        Assert.assertEquals(2.0, q1VcpuUsed, delta);
        Assert.assertEquals(4.0, q2VcpuUsed, delta);
        Assert.assertEquals(6.0, q3VcpuUsed, delta);
    }

    @Test
    public void testUsedValueWithoutConsumer() {
        final long sellerOid = 111L;
        final TopologyEntity.Builder providerBuilder = createSellerTopologyEntity(sellerOid);
        final TopologyEntity provider = providerBuilder.build();
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        stitchOperation.performOperation(
                Stream.of(provider), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        final List<CommoditySoldDTO> commoditySoldDTOList =
                provider.getTopologyEntityDtoBuilder().build().getCommoditySoldListList();
        final double q1VcpuUsed = getCommodityUsedValue(commoditySoldDTOList, COMM_TYPES.get(0).getNumber());
        final double q2VcpuUsed = getCommodityUsedValue(commoditySoldDTOList, COMM_TYPES.get(1).getNumber());
        final double q3VcpuUsed = getCommodityUsedValue(commoditySoldDTOList, COMM_TYPES.get(2).getNumber());
        Assert.assertEquals(0.0, q1VcpuUsed, delta);
        Assert.assertEquals(0.0, q2VcpuUsed, delta);
        Assert.assertEquals(0.0, q3VcpuUsed, delta);
    }

    private TopologyEntity seller() {
        final long sellerOid = 111L;
        final TopologyEntity.Builder seller = createSellerTopologyEntity(sellerOid);
        IntStream.range(0, COMM_TYPES.size()).forEach(index -> {
            final List<Double> usedValueList = usedValues.get(index);
            final CommodityType commodityType = COMM_TYPES.get(index);
            usedValueList.forEach(usedValue -> {
                final CommodityBoughtDTO commBoughtInclude =
                        PostStitchingTestUtilities.makeCommodityBought(commodityType).toBuilder()
                                .setUsed(usedValue)
                                .build();
                final CommodityBoughtDTO commBoughtExclude =
                        PostStitchingTestUtilities.makeCommodityBought(commodityTypeExclude).toBuilder()
                                .setUsed(usedValue * 10)
                                .build();
                final TopologyEntity.Builder buyer =
                        PostStitchingTestUtilities.makeTopologyEntityBuilder(
                                0,
                                EntityType.VIRTUAL_MACHINE_VALUE,
                                Collections.EMPTY_LIST,
                                ImmutableMap.of(sellerOid,
                                        Lists.newArrayList(commBoughtExclude, commBoughtInclude)));
                seller.addConsumer(buyer);
            });
        });
        return seller.build();
    }

    private TopologyEntity.Builder createSellerTopologyEntity(final long sellerOid) {
        final List<CommoditySoldDTO> commoditySoldDTOList = COMM_TYPES.stream()
                .map(commodityType -> PostStitchingTestUtilities.makeCommoditySold(commodityType))
                .collect(Collectors.toList());

        return PostStitchingTestUtilities.makeTopologyEntityBuilder(
                        sellerOid,
                        EntityType.PHYSICAL_MACHINE_VALUE,
                        commoditySoldDTOList,
                        Collections.EMPTY_LIST
                );
    }

    private double getCommodityUsedValue(List<CommoditySoldDTO> commoditySoldDTOList, int commodityType) {
        return commoditySoldDTOList.stream()
                .filter(comm -> comm.getCommodityType().getType() == commodityType)
                .map(comm -> comm.getUsed())
                .findFirst()
                .orElse(0.0);
    }
}
