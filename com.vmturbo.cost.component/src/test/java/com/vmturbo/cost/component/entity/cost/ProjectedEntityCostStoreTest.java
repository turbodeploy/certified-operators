package com.vmturbo.cost.component.entity.cost;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Unit tests for {@link ProjectedEntityCostStore}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class ProjectedEntityCostStoreTest {

    private ProjectedEntityCostStore store;

    private static final EntityCost COST_7L = EntityCost.newBuilder()
            .setAssociatedEntityId(7L)
            .setAssociatedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addComponentCost(ComponentCost.newBuilder()
                    .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                    .setAmount(CurrencyAmount.newBuilder()
                            .setAmount(100).setCurrency(840)))
            .addComponentCost(ComponentCost.newBuilder()
                    .setCategory(CostCategory.IP)
                    .setAmount(CurrencyAmount.newBuilder()
                            .setAmount(200).setCurrency(840)))
            .build();


    private static final EntityCost COST_9L = EntityCost.newBuilder()
            .setAssociatedEntityId(9L)
            .setAssociatedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addComponentCost(ComponentCost.newBuilder()
                    .setCategory(CostCategory.IP)
                    .setAmount(CurrencyAmount.newBuilder()
                            .setAmount(100).setCurrency(840)))
            .build();

    private static final EntityCost COST_8L = EntityCost.newBuilder()
            .setAssociatedEntityId(8L)
            .setAssociatedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addComponentCost(ComponentCost.newBuilder()
                    .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                    .setAmount(CurrencyAmount.newBuilder()
                            .setAmount(1.1).setCurrency(840)))
            .build();

    @Before
    public void setup() {
        store = new ProjectedEntityCostStore();
        store.updateProjectedEntityCosts(Arrays.asList(COST_7L, COST_9L, COST_8L));
    }

    @Test
    public void testUpdateAndGet() {
        store.updateProjectedEntityCosts(Arrays.asList(COST_7L));
        final Collection<StatRecord> retCostMap =
                store.getProjectedStatRecords(Collections.singleton(COST_7L.getAssociatedEntityId()));
        assertThat(retCostMap, is(EntityCostToStatRecordConverter.convertEntityToStatRecord(COST_7L)));
        assertThat(retCostMap.stream().mapToDouble(i2 ->
                        i2.getValues().getTotal()).sum(),
                is(COST_7L.getComponentCostList().stream().mapToDouble(i1 -> i1.getAmount().getAmount()).sum()));
    }

    @Test
    public void testWithFilterIds() {
        store.updateProjectedEntityCosts(Arrays.asList(COST_7L, COST_9L, COST_8L));
        Collection<StatRecord> result = store.getProjectedStatRecords(Collections.singleton(7L));
        assertThat("Incorrect number of result. Expected 1 entry only.",
                result.size(), is(2));
        ArrayList<StatRecord> listOfStatRecord = Lists.newArrayList();
        listOfStatRecord.addAll(EntityCostToStatRecordConverter.convertEntityToStatRecord(COST_7L));
        assertThat(result.stream().filter(i1 -> i1.getAssociatedEntityId() == 7L).collect(toList()),
                is(listOfStatRecord));
    }

    @Test
    //entityType
    public void testWith1Group1() {
        List<EntityCost> listOfEntityCost = Arrays.asList(COST_7L, COST_9L, COST_8L);
        store.updateProjectedEntityCosts(listOfEntityCost);
        Collection<StatRecord> result = store.getProjectedStatRecordsByGroup(
                Collections.singletonList(GroupBy.ENTITY_TYPE));
        assertThat(result.size(), is(listOfEntityCost.stream().map(EntityCost::getAssociatedEntityType)
                .collect(toSet()).size()));
        List<StatRecord> statRecordForId = result.stream().filter(i1 ->
                EntityType.VIRTUAL_MACHINE_VALUE == i1.getAssociatedEntityType())
                .collect(toList());
        assertThat(statRecordForId.size(), is(1));
        assertEquals(statRecordForId.stream()
                .mapToDouble(i2 -> i2.getValues().getTotal()).sum(), 401.1F, 0.001F);
        //check if all the stats have legit numbers..
        checkForNonZeroAmounts(result);
    }

    @Test
    //entityId
    public void testWith1Group2() {
        List<EntityCost> listOfEntityCost = Arrays.asList(COST_7L, COST_9L, COST_8L);
        store.updateProjectedEntityCosts(listOfEntityCost);
        Collection<StatRecord> result = store.getProjectedStatRecordsByGroup(
                Collections.singletonList(GroupBy.ENTITY));
        List<StatRecord> statRecordForId = result.stream().filter(i1 ->
                i1.getAssociatedEntityId() == COST_7L.getAssociatedEntityId())
                .collect(toList());
        assertThat(statRecordForId.size(), is(1));
        assertThat(result.size(), is(listOfEntityCost.size()));
        assertThat(statRecordForId
                .stream()
                .findFirst()
                .map(i2 -> i2.getValues().getTotal()).get(), is(300F));
        checkForNonZeroAmounts(result);
    }

    @Test
    //cost category
    public void testWith1Group3() {
        List<EntityCost> listOfEntityCost = Arrays.asList(COST_7L, COST_9L, COST_8L);
        store.updateProjectedEntityCosts(listOfEntityCost);
        Collection<StatRecord> result = store.getProjectedStatRecordsByGroup(
                Collections.singletonList(GroupBy.COST_CATEGORY));
        List<StatRecord> statRecordForId = result.stream().filter(i1 ->
                i1.getCategory().equals(CostCategory.IP))
                .collect(toList());
        double ipCosts = listOfEntityCost.stream().mapToDouble(i1 -> i1.getComponentCostList().stream()
                .filter(i2 -> i2.getCategory().equals(CostCategory.IP))
                .mapToDouble(i2 -> i2.getAmount().getAmount()).sum()).sum();

        double onDemandCost = listOfEntityCost.stream().mapToDouble(i1 -> i1.getComponentCostList().stream()
                .filter(i2 -> i2.getCategory().equals(CostCategory.ON_DEMAND_COMPUTE))
                .mapToDouble(i2 -> i2.getAmount().getAmount()).sum()).sum();

        double totalCostForCurrentStats = statRecordForId.stream().mapToDouble(i1 -> i1.getValues().getTotal()).sum();
        assertThat(totalCostForCurrentStats, is(ipCosts));
        assertThat(totalCostForCurrentStats, not(onDemandCost));
        assertThat(totalCostForCurrentStats, not(onDemandCost + ipCosts));
    }

    @Test
    //cost category+entityType
    public void testWith2Group1() {
        List<EntityCost> listOfEntityCost = Arrays.asList(COST_7L, COST_9L, COST_8L);
        store.updateProjectedEntityCosts(listOfEntityCost);
        Collection<StatRecord> result = store.getProjectedStatRecordsByGroup(
                ImmutableList.of(GroupBy.COST_CATEGORY, GroupBy.ENTITY_TYPE));
        assertThat(result.size(), is(2));
        // this eg works out because all the entity type is 10.
        List<StatRecord> statRecordForIp = result.stream().filter(i1 ->
                i1.getCategory().equals(CostCategory.IP)).collect(toList());

        List<StatRecord> statRecordForOnDemandCost = result.stream().filter(i1 ->
                i1.getCategory().equals(CostCategory.ON_DEMAND_COMPUTE))
                .collect(toList());
        double ipCosts = listOfEntityCost.stream().mapToDouble(i1 -> i1.getComponentCostList().stream()
                .filter(i2 -> i2.getCategory().equals(CostCategory.IP))
                .mapToDouble(i2 -> i2.getAmount().getAmount()).sum()).sum();

        double onDemandCost = listOfEntityCost.stream().mapToDouble(i1 -> i1.getComponentCostList().stream()
                .filter(i2 -> i2.getCategory().equals(CostCategory.ON_DEMAND_COMPUTE))
                .mapToDouble(i2 -> i2.getAmount().getAmount()).sum()).sum();

        assertThat(statRecordForIp.stream().mapToDouble(i1 -> i1.getValues().getTotal()).sum(), closeTo(ipCosts, 0.1));
        assertThat(statRecordForOnDemandCost.stream().mapToDouble(i1 -> i1.getValues().getTotal()).sum(),
                closeTo(onDemandCost, 0.1));
        checkForNonZeroAmounts(result);
    }

    @Test
    //cost category + entity
    public void testWith2Group2() {
        List<EntityCost> listOfEntityCost = Arrays.asList(COST_7L, COST_9L, COST_8L);
        store.updateProjectedEntityCosts(listOfEntityCost);
        Collection<StatRecord> result = store.getProjectedStatRecordsByGroup(
                ImmutableList.of(GroupBy.COST_CATEGORY, GroupBy.ENTITY));
        assertThat(result.size(), is(4));
        checkForNonZeroAmounts(result);
    }

    @Test
    //entityType+ entityId
    public void testWith2Group3() {
        List<EntityCost> listOfEntityCost = Arrays.asList(COST_7L, COST_9L, COST_8L);
        store.updateProjectedEntityCosts(listOfEntityCost);
        Collection<StatRecord> result = store.getProjectedStatRecordsByGroup(
                ImmutableList.of(GroupBy.ENTITY_TYPE, GroupBy.ENTITY));
        //because all entityType = VM, we have 3 groups based on entityId.
        assertThat(result.size(), is(3));
        List<StatRecord> statRecordForId = result.stream().filter(i1 ->
                i1.getAssociatedEntityId() == COST_7L.getAssociatedEntityId())
                .collect(toList());
        assertThat(statRecordForId.size(), is(1));
        assertThat(result.size(), is(listOfEntityCost.size()));
        assertThat(statRecordForId
                .stream()
                .findFirst()
                .map(i2 -> i2.getValues().getTotal()).get(), is(300F));
        checkForNonZeroAmounts(result);
    }


    @Test
    public void testWithMoreThan2GroupBy() {
        List<EntityCost> listOfEntityCost = Arrays.asList(COST_7L, COST_9L, COST_8L);
        store.updateProjectedEntityCosts(listOfEntityCost);
        Collection<StatRecord> result = store.getProjectedStatRecordsByGroup(
                ImmutableList.of(GroupBy.ENTITY_TYPE, GroupBy.ENTITY, GroupBy.COST_CATEGORY));
        Collection<StatRecord> allProjectedEntityStore = store.getProjectedStatRecords(Collections.emptySet());
        assertThat(allProjectedEntityStore, is(result));
    }

    @Test
    public void testGetEmpty() {
        store.updateProjectedEntityCosts(Arrays.asList(COST_7L));
        assertThat(store.getProjectedStatRecords(Collections.emptySet()), is(Collections.emptyList()));
    }

    @Test
    public void testGetMissing() {
        store.updateProjectedEntityCosts(Arrays.asList(COST_7L));
        assertThat(store.getProjectedStatRecords(Collections.singleton(1 + COST_7L.getAssociatedEntityId())),
                is(Collections.emptyList()));
    }

    @Test
    public void testGetProjectedEntityCosts() {
        store.updateProjectedEntityCosts(Arrays.asList(COST_7L));
        assertThat(store.getProjectedEntityCosts(Collections.singleton(1 + COST_7L.getAssociatedEntityId())),
                is(Collections.emptyMap()));
    }

    private void checkForNonZeroAmounts(final Collection<StatRecord> result) {
        assertTrue(result.stream().noneMatch(i1 -> i1.getValues().getTotal() == 0));
    }
}
