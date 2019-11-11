package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.spy;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;

import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;


/**
 * Unit tests for {@link ProjectedRICoverageAndUtilStore}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class ProjectedRICoverageAndUtilStoreTest {

    private RepositoryServiceMole repositoryService = spy(new RepositoryServiceMole());
    private GrpcTestServer testServer = GrpcTestServer.newServer(repositoryService);
    private ProjectedRICoverageAndUtilStore store;

    private static final EntityReservedInstanceCoverage ENTITY_RI_COVERAGE =
        EntityReservedInstanceCoverage.newBuilder()
            .setEntityId(1L)
            .putCouponsCoveredByRi(10L, 100.0)
            .build();
    @Before
    public void setup() throws Exception {
        testServer.start();
        store = Mockito.spy(new ProjectedRICoverageAndUtilStore(RepositoryServiceGrpc
                                                                .newBlockingStub(testServer.getChannel())));
    }

    @Test
    public void testUpdateAndGet() {
        store.updateProjectedRICoverage(Arrays.asList(ENTITY_RI_COVERAGE));
        final Map<Long, Map<Long, Double>> retCostMap =
                store.getAllProjectedEntitiesRICoverages();
        assertThat(retCostMap, is(ImmutableMap.of(
                        ENTITY_RI_COVERAGE.getEntityId(),
                        ENTITY_RI_COVERAGE.getCouponsCoveredByRiMap())));
    }

    @Test
    public void testGetEmpty() {
        assertThat(store.getAllProjectedEntitiesRICoverages(),
                is(Collections.emptyMap()));
    }
}
