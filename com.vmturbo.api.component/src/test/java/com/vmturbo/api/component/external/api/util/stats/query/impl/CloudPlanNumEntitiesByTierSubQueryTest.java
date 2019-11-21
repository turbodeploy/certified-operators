package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This class tests the functionality in CloudPlanNumEntitiesByTierSubQuery.java.
 *
 */
public class CloudPlanNumEntitiesByTierSubQueryTest {
    @Mock
    private SupplyChainFetcherFactory supplyChainFetcherFactory;

    CloudPlanNumEntitiesByTierSubQuery query;

    @Mock
    private RepositoryApi repositoryApi;

    /**
     * Initializations to run before the tests in this class.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        query = new CloudPlanNumEntitiesByTierSubQuery(repositoryApi, supplyChainFetcherFactory, 777777);
    }

    /**
     * The Map, CloudNumPlanEntitiesByTierSubQuery.ENTITY_TYPE_TO_GET_TIER_FUNCTION, is used for
     * mapping the number of providers by tier type.
     *
     * <p>This tests tests the lambda function that gets the tier id and the number of entities (providers) by tier type,
     * from a given TopologyEntityDTO.
     */
    @Test
    public void testEntityTypeToGetTierFunction() {
        final RelatedEntity storage =   RelatedEntity.newBuilder()
                        .setEntityType(EntityType.STORAGE_TIER_VALUE)
                        .setOid(7777L)
                        .build();
        final TopologyEntityDTO topologyEntityVM =   TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setOid(77777L)
                        .build();
        final ApiPartialEntity virtualVolume1 =   ApiPartialEntity.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .setOid(777777L)
                        .build();
        final ApiPartialEntity virtualVolume2 =   ApiPartialEntity.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .addProviders(storage)
                        .setOid(7777777L)
                        .build();

        // Add and retrieve entries from CloudNumPlanEntitiesByTierSubQuery.ENTITY_TYPE_TO_GET_TIER_FUNCTION
        // Test that the assertions pass without exceptions whether the provider type is present or
        // not.  Test the lambda function that computes the number of entities by tier type.
        Map<Long, ApiPartialEntity> entities = new HashMap<>();
        String volumeEntityType = UIEntityType.VIRTUAL_VOLUME.apiStr();
        entities.put(7777777L, virtualVolume1);
        Map<Optional<Long>, Long> tierIdToNumEntities = entities.values().stream()
                        .collect(Collectors.groupingBy(CloudPlanNumEntitiesByTierSubQuery.ENTITY_TYPE_TO_GET_TIER_FUNCTION.get(volumeEntityType), Collectors.counting()));

        // When there are no providers for VIRTUAL_VOLUME, the function to map tier id to number of providers returns 0.
        assertEquals(1, tierIdToNumEntities.size());
        assertEquals(null, tierIdToNumEntities.get(null));

        tierIdToNumEntities.clear();
        entities.clear();
        entities.put(7777777L, virtualVolume2);
        tierIdToNumEntities = entities.values().stream()
                        .collect(Collectors.groupingBy(CloudPlanNumEntitiesByTierSubQuery.ENTITY_TYPE_TO_GET_TIER_FUNCTION.get(volumeEntityType), Collectors.counting()));
        // When there are is a provider providers for VIRTUAL_VOLUME, the function to map tier id to number of providers returns the count of providers.
        assertEquals(1, tierIdToNumEntities.size());
        assertEquals(Long.valueOf(1), tierIdToNumEntities.get(Optional.of(7777L)));
    }
}
