package com.vmturbo.topology.processor.history;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.mockito.Mockito;

import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.PerTargetEntityInformationImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.DiscoveryOriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.OriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.UtilizationDataImpl;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Base functionality for mocking topology graph for historical editing tests.
 */
public abstract class BaseGraphRelatedTest {

    private static final long TARGET_ID = 1L;

    /**
     * Mock the topology graph from the entities. Only the entity retrieval.
     *
     * @param entities collection of entities
     * @return mock of the graph
     */
    @Nonnull
    protected static TopologyGraph<TopologyEntity> mockGraph(@Nonnull Set<TopologyEntity> entities) {
        @SuppressWarnings("unchecked")
        TopologyGraph<TopologyEntity> graph = Mockito.mock(TopologyGraph.class);
        Mockito.when(graph.entities()).thenReturn(entities.stream());
        Mockito.when(graph.size()).thenReturn(entities.size());
        for (TopologyEntity entity : entities) {
            Mockito.when(graph.getEntity(entity.getOid())).thenReturn(Optional.of(entity));
        }
        return graph;
    }

    /**
     * Mock an entity with one sold and optional 1 bought commodity.
     *
     * @param type entity type
     * @param oid entity oid
     * @param ctSold sold commodity type
     * @param capacitySold sold capacity
     * @param usedSold sold usage
     * @param provider bought commodity provider oid
     * @param ctBought bought commodity type
     * @param usedBought bought usage
     * @param utilizationData sold commodity utilization data
     * @param resizable whether sold commodity is resizable
     * @return mocked entity
     */
    @Nonnull
    protected static TopologyEntity mockEntity(int type, long oid, @Nonnull CommodityTypeImpl ctSold,
                    double capacitySold, @Nullable Double usedSold, @Nullable Long provider,
                                               @Nullable CommodityTypeImpl ctBought,
                    @Nullable Double usedBought, @Nullable UtilizationDataImpl utilizationData,
                    boolean resizable) {
        TopologyEntity e = Mockito.mock(TopologyEntity.class);
        Mockito.when(e.getEntityType()).thenReturn(type);
        Mockito.when(e.getOid()).thenReturn(oid);
        Mockito.when(e.getClonedFromEntity()).thenReturn(Optional.empty());
        TopologyEntityImpl entityImpl = new TopologyEntityImpl();
        entityImpl.setOid(oid).setEntityType(type);
        final CommoditySoldImpl commSold = new CommoditySoldImpl().setCommodityType(ctSold)
                .setCapacity(capacitySold)
                .setIsResizeable(resizable);
        entityImpl.addCommoditySoldList(commSold);
        if (usedSold != null) {
            commSold.setUsed(usedSold);
        }
        if (utilizationData != null) {
            commSold.setUtilizationData(utilizationData);
        }
        if (provider != null) {
            entityImpl.addCommoditiesBoughtFromProviders(
                    new CommoditiesBoughtFromProviderImpl()
                            .setProviderId(provider)
                            .addCommodityBought(new CommodityBoughtImpl()
                                            .setCommodityType(ctBought)
                                            .setUsed(usedBought)));
        }
        Mockito.when(e.getTopologyEntityImpl()).thenReturn(entityImpl);
        return e;
    }

    /**
     * Create a setting object for spec and value.
     *
     * @param entityOid entity
     * @param value setting value
     * @param entitySettingSpecs setting definition
     * @return setting instance
     */
    @Nonnull
    public static EntitySettings createEntitySetting(long entityOid,
                                                        long value,
                                                        @Nonnull EntitySettingSpecs entitySettingSpecs) {
        return EntitySettings.newBuilder()
                        .setEntityOid(entityOid)
                        .setDefaultSettingPolicyId(1)
                        .addUserSettings(SettingToPolicyId.newBuilder()
                                        .setSetting(Setting.newBuilder()
                                                        .setSettingSpecName(entitySettingSpecs
                                                                        .getSettingName())
                                                        .setNumericSettingValue(
                                                                                NumericSettingValue
                                                                                                .newBuilder()
                                                                                                .setValue(value))
                                                        .build())
                                        .addSettingPolicyId(1)
                                        .build())
                        .build();
    }

    /**
     * Create an entity and add to the topology and settings maps.
     *
     * @param entityOid entity id
     * @param entityType entity type
     * @param entitySettingSpecs setting definition
     * @param value setting value
     * @param topologyBuilderMap topology map
     * @param entitySettings settings map
     */
    protected static void addEntityWithSetting(long entityOid, int entityType,
                                               @Nonnull EntitySettingSpecs entitySettingSpecs,
                                               long value,
                                               @Nonnull Map<Long, TopologyEntity.Builder> topologyBuilderMap,
                                               @Nonnull Map<Long, EntitySettings> entitySettings) {
        topologyBuilderMap.put(entityOid, TopologyEntity.newBuilder(new TopologyEntityImpl()
                .setOid(entityOid)
                .setEntityType(entityType)
                .setOrigin(new OriginImpl()
                        .setDiscoveryOrigin(new DiscoveryOriginImpl()
                                .putDiscoveredTargetData(TARGET_ID,
                                        new PerTargetEntityInformationImpl()
                                                .setOrigin(EntityOrigin.DISCOVERED))))));
        entitySettings.put(entityOid,
                           createEntitySetting(entityOid, value, entitySettingSpecs));
    }

}
