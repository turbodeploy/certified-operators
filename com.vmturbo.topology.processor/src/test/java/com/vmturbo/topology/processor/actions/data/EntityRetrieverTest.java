package com.vmturbo.topology.processor.actions.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.conversions.TopologyToSdkEntityConverter;
import com.vmturbo.topology.processor.topology.pipeline.CachedTopology;
import com.vmturbo.topology.processor.topology.pipeline.CachedTopology.CachedTopologyResult;

/**
 * Unit test for {@link EntityRetriever}.
 */
public class EntityRetrieverTest {

    private static final long ENTITY1_OID = 1001L;
    private static final long ENTITY2_OID = 1002L;
    private static final long ENTITY3_OID = 1003L;

    private static final TopologyEntityDTO ENTITY_1 = TopologyEntityDTO.newBuilder().setEntityType(
            1).setOid(ENTITY1_OID).build();
    private static final TopologyEntityDTO ENTITY_2 = TopologyEntityDTO.newBuilder().setEntityType(
            2).setOid(ENTITY2_OID).build();

    private EntityRetriever entityRetriever;
    private Map<Long, TopologyEntity.Builder> cachedEntities;
    private List<TopologyEntityDTO> repoData;

    /**
     * Initializes the tests.
     */
    @Before
    public void init() {
        final TopologyToSdkEntityConverter entityConverter = Mockito.mock(
                TopologyToSdkEntityConverter.class);
        final RepositoryClient repositoryClient = Mockito.mock(RepositoryClient.class);
        final CachedTopology cachedTopology = Mockito.mock(CachedTopology.class);
        entityRetriever = new EntityRetriever(entityConverter, repositoryClient, cachedTopology, 1);
        cachedEntities = new HashMap<>();
        repoData = new ArrayList<>();
        final CachedTopologyResult cachedTopologyResult = Mockito.mock(CachedTopologyResult.class);
        Mockito.when(cachedTopologyResult.getEntities()).thenReturn(cachedEntities);
        Mockito.when(cachedTopology.getTopology(Mockito.any())).thenReturn(cachedTopologyResult);
        Mockito.when(repositoryClient.retrieveTopologyEntities(Mockito.any(), Mockito.anyLong()))
                .thenAnswer(invocation -> {
                    @SuppressWarnings("unchecked")
                    final List<Long> oids = (List<Long>)invocation.getArguments()[0];
                    return repoData.stream().filter(entity -> oids.contains(entity.getOid()));
                }
        );
    }

    /**
     * Tests retrieving entity from cache. Repository is intact.
     */
    @Test
    public void testLoadFromCacheOnly()  {
        putIntoCache(ENTITY_1);
        final Optional<TopologyEntityDTO> topologyEntityDTOOptional =
                entityRetriever.retrieveTopologyEntity(ENTITY1_OID);
        Assert.assertEquals(Optional.of(ENTITY_1), topologyEntityDTOOptional);
    }

    /**
     * Tests mixed load. Something is available from the cache, something - loaded from repository.
     */
    @Test
    public void testLoadMixedFromCacheAndRepo() {
        putIntoCache(ENTITY_1);
        repoData.add(ENTITY_2);
        final List<TopologyEntityDTO> topologyEntities =
                entityRetriever.retrieveTopologyEntities(Arrays.asList(ENTITY1_OID, ENTITY2_OID));
        final Optional<TopologyEntityDTO> entity1 = topologyEntities.stream().filter(
                entity -> entity.getOid() == ENTITY1_OID).findFirst();
        Assert.assertEquals(Optional.of(ENTITY_1), entity1);
        final Optional<TopologyEntityDTO> entity2 = topologyEntities.stream().filter(
                entity -> entity.getOid() == ENTITY2_OID).findFirst();
    }

    /**
     * Tests when entity is not found in neither repository nor cached topology.
     */
    @Test
    public void testMiss() {
        putIntoCache(ENTITY_1);
        repoData.add(ENTITY_2);
        final Optional<TopologyEntityDTO> entityOptional = entityRetriever.retrieveTopologyEntity(
                ENTITY3_OID);
        Assert.assertEquals(Optional.empty(), entityOptional);
    }

    private void putIntoCache(@Nonnull TopologyEntityDTO topologyEntityDTO) {
        final TopologyEntity.Builder topologyEntity = TopologyEntity.newBuilder(
                topologyEntityDTO.toBuilder());
        cachedEntities.put(topologyEntityDTO.getOid(), topologyEntity);
    }
}
