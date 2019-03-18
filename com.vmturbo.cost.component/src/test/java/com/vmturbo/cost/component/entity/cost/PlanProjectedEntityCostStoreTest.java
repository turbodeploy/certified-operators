package com.vmturbo.cost.component.entity.cost;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedEntityCostRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class PlanProjectedEntityCostStoreTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;
    private Flyway flyway;
    private PlanProjectedEntityCostStore store;
    private DSLContext dsl;
    private final int chunkSize = 10;
    private static final EntityCost COST = EntityCost.newBuilder()
                    .setAssociatedEntityId(7L)
                    .setAssociatedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addComponentCost(ComponentCost.newBuilder()
                    .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                    .setAmount(CurrencyAmount.newBuilder()
                            .setAmount(100).setCurrency(840)))
            .build();

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        store = new PlanProjectedEntityCostStore(dsl, chunkSize);
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testUpdateProjectedEntityCostsTable() {
        TopologyInfo topoInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(1l)
                .setTopologyId(0l)
                .setTopologyType(TopologyType.PLAN)
                .build();
        List<EntityCost> cost = Arrays.asList(COST);
        store.updatePlanProjectedEntityCostsTableForPlan(topoInfo, cost);
        final List<PlanProjectedEntityCostRecord> records = dsl
                .selectFrom(Tables.PLAN_PROJECTED_ENTITY_COST).fetch();
        assertEquals(1, records.size());
        PlanProjectedEntityCostRecord rcd = records.get(0);
        assertTrue(rcd.getPlanId() == 1);
        assertTrue(rcd.getAssociatedEntityId() == 7);
        assertTrue(rcd.getEntityCost().getAssociatedEntityType() == EntityType.VIRTUAL_MACHINE_VALUE);
        assertTrue(rcd.getEntityCost().getComponentCostList().get(0).getCategory() == CostCategory.ON_DEMAND_COMPUTE);
        assertTrue(rcd.getEntityCost().getComponentCostList().get(0).getAmount().getCurrency() == 840);
        assertTrue(100.0000 == rcd.getEntityCost().getComponentCostList().get(0).getAmount().getAmount()); 
    }
}
