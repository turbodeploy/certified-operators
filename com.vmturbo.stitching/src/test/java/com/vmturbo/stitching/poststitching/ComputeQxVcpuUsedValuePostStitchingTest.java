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

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

public class ComputeQxVcpuUsedValuePostStitchingTest {
    private static String key = "123";
    private static CommodityType q1Vcpu = TopologyDTO.CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.Q1_VCPU.getNumber()).setKey(key).build();
    private static CommodityType q2Vcpu = TopologyDTO.CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.Q2_VCPU.getNumber()).setKey(key).build();
    private static CommodityType q3Vcpu = TopologyDTO.CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.Q3_VCPU.getNumber()).setKey(key).build();
    private static final List<CommodityType> COMM_TYPES = ImmutableList.of(q1Vcpu,
        q2Vcpu, q3Vcpu);
    private static final List<List<Double>> usedValues =
        ImmutableList.of(ImmutableList.of(1.0, 2.0, 3.0),
            ImmutableList.of(4.0, 4.0, 4.0), ImmutableList.of(5.0, 6.0, 7.0));
    private static final CommodityDTO.CommodityType commodityTypeExclude =
        CommodityDTO.CommodityType.CPU;
    private final ComputedQxVcpuUsedValuePostStitchingOperation stitchOperation =
            new ComputedQxVcpuUsedValuePostStitchingOperation();

    private final IStitchingJournal journal = mock(IStitchingJournal.class);

    final double delta = 0.0000001;


    /***
     * Tests setting the used value for QnVcpu.
     */
    @Test
    public void testUsedValue() {
        TopologyEntity provider = seller();

        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        stitchOperation.performOperation(
                Stream.of(provider), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        final List<CommoditySoldDTO> commoditySoldDTOList =
                provider.getTopologyEntityDtoBuilder().build().getCommoditySoldListList();
        final double q1VcpuUsed = getCommodityUsedValue(commoditySoldDTOList, COMM_TYPES.get(0));
        final double q2VcpuUsed = getCommodityUsedValue(commoditySoldDTOList, COMM_TYPES.get(1));
        final double q3VcpuUsed = getCommodityUsedValue(commoditySoldDTOList, COMM_TYPES.get(2));
        Assert.assertEquals(2.0, q1VcpuUsed, delta);
        Assert.assertEquals(4.0, q2VcpuUsed, delta);
        Assert.assertEquals(6.0, q3VcpuUsed, delta);
    }

    /***
     * Tests setting the max value for QnVcpu.
     */
    @Test
    public void testMaxValue() {
        TopologyEntity provider = seller();

        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        stitchOperation.performOperation(
            Stream.of(provider), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        final List<CommoditySoldDTO> commoditySoldDTOList =
            provider.getTopologyEntityDtoBuilder().build().getCommoditySoldListList();
        final double q1VcpuMax = getCommodityMaxValue(commoditySoldDTOList,
            COMM_TYPES.get(0));
        final double q2VcpuMax = getCommodityMaxValue(commoditySoldDTOList,
            COMM_TYPES.get(1));
        final double q3VcpuMax = getCommodityMaxValue(commoditySoldDTOList,
            COMM_TYPES.get(2));
        Assert.assertEquals(3.0, q1VcpuMax, delta);
        Assert.assertEquals(4.0, q2VcpuMax, delta);
        Assert.assertEquals(7.0, q3VcpuMax, delta);
    }

    /***
     * Tests setting the used value for QnVcpu without a consumer.
     */
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
        final double q1VcpuUsed = getCommodityUsedValue(commoditySoldDTOList, COMM_TYPES.get(0));
        final double q2VcpuUsed = getCommodityUsedValue(commoditySoldDTOList, COMM_TYPES.get(1));
        final double q3VcpuUsed = getCommodityUsedValue(commoditySoldDTOList, COMM_TYPES.get(2));
        Assert.assertEquals(0.0, q1VcpuUsed, delta);
        Assert.assertEquals(0.0, q2VcpuUsed, delta);
        Assert.assertEquals(0.0, q3VcpuUsed, delta);
    }

    /***
     * Tests setting the max value for QnVcpu without a consumer.
     */
    @Test
    public void testMaxValueWithoutConsumer() {
        final long sellerOid = 111L;
        final TopologyEntity.Builder providerBuilder = createSellerTopologyEntity(sellerOid);
        final TopologyEntity provider = providerBuilder.build();
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        stitchOperation.performOperation(
            Stream.of(provider), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        final List<CommoditySoldDTO> commoditySoldDTOList =
            provider.getTopologyEntityDtoBuilder().build().getCommoditySoldListList();
        final double q1VcpuMax = getCommodityUsedValue(commoditySoldDTOList,
            COMM_TYPES.get(0));
        final double q2VcpuMax = getCommodityUsedValue(commoditySoldDTOList,
            COMM_TYPES.get(1));
        final double q3VcpuMax = getCommodityUsedValue(commoditySoldDTOList,
            COMM_TYPES.get(2));
        Assert.assertEquals(0.0, q1VcpuMax, delta);
        Assert.assertEquals(0.0, q2VcpuMax, delta);
        Assert.assertEquals(0.0, q3VcpuMax, delta);
    }

    private TopologyEntity seller() {
        final long sellerOid = 111L;
        final TopologyEntity.Builder seller = createSellerTopologyEntity(sellerOid);
        IntStream.range(0, COMM_TYPES.size()).forEach(index -> {
            final List<Double> commodityValuesList = usedValues.get(index);
            final CommodityType commodityType = COMM_TYPES.get(index);
            commodityValuesList.forEach(commodityValue -> {
                final CommodityBoughtDTO commBoughtInclude =
                        PostStitchingTestUtilities.makeCommodityBought(CommodityDTO.CommodityType.forNumber(commodityType.getType()), key).toBuilder()
                                .setUsed(commodityValue)
                                .setPeak(commodityValue)
                                .build();
                final CommodityBoughtDTO commBoughtExclude =
                        PostStitchingTestUtilities.makeCommodityBought(commodityTypeExclude, key).toBuilder()
                                .setUsed(commodityValue * 10)
                                .setPeak(commodityValue)
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
                .map(commodityType -> PostStitchingTestUtilities.makeCommoditySold(CommodityDTO.CommodityType.forNumber(commodityType.getType()), key))
                .collect(Collectors.toList());

        return PostStitchingTestUtilities.makeTopologyEntityBuilder(
                        sellerOid,
                        EntityType.PHYSICAL_MACHINE_VALUE,
                        commoditySoldDTOList,
                        Collections.EMPTY_LIST
                );
    }

    private double getCommodityUsedValue(List<CommoditySoldDTO> commoditySoldDTOList, CommodityType commodityType) {
        return commoditySoldDTOList.stream()
                .filter(comm -> comm.getCommodityType().equals(commodityType))
                .map(CommoditySoldDTO::getUsed)
                .findFirst()
                .orElse(0.0);
    }

    private double getCommodityMaxValue(List<CommoditySoldDTO> commoditySoldDTOList,
                                      CommodityType commodityType) {
        return commoditySoldDTOList.stream()
            .filter(comm -> comm.getCommodityType().equals(commodityType))
            .map(CommoditySoldDTO::getPeak)
            .findFirst()
            .orElse(0.0);
    }
}
