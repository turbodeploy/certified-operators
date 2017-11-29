package com.vmturbo.topology.processor.stitching;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;

/**
 * Utilities for generating data used in stitching tests.
 */
public class StitchingTestUtils {
    @Nonnull
    public static StitchingDataAllowingTargetChange stitchingData(@Nonnull final String localId,
                                                    @Nonnull final List<String> providerIds) {
        return stitchingData(localId, EntityType.VIRTUAL_MACHINE, providerIds);
    }

    @Nonnull
    public static StitchingDataAllowingTargetChange stitchingData(@Nonnull final String localId,
                                                                  final EntityType entityType,
                                                                  @Nonnull final List<String> providerIds) {
        final EntityDTO.Builder builder = EntityDTO.newBuilder()
            .setId(localId)
            .setEntityType(entityType);

        for (String providerId : providerIds) {
            builder.addCommoditiesBought(CommodityBought.newBuilder()
                .setProviderId(providerId)
                .addBought(CommodityDTO.newBuilder().setCommodityType(CommodityType.CPU))
            );
        }

        return stitchingData(builder);
    }

    @Nonnull
    public static StitchingDataAllowingTargetChange stitchingData(@Nonnull final EntityDTO.Builder builder) {
        final long DEFAULT_TARGET_ID = 12345L;

        return new StitchingDataAllowingTargetChange(builder, DEFAULT_TARGET_ID, IdentityGenerator.next());
    }

    @Nonnull
    public static Map<String, StitchingEntityData> topologyMapOf(@Nonnull final StitchingEntityData... entities) {
        final Map<String, StitchingEntityData> map = new HashMap<>(entities.length);
        for (StitchingEntityData entity : entities) {
            map.put(entity.getEntityDtoBuilder().getId(), entity);
        }

        return map;
    }

    public static class StitchingDataAllowingTargetChange extends StitchingEntityData {
        public StitchingDataAllowingTargetChange(@Nonnull final EntityDTO.Builder entityDtoBuilder,
                                                 final long targetId,
                                                 final long oid) {
            super(entityDtoBuilder, targetId, oid, 0);
        }

        public StitchingEntityData forTarget(final long targetId) {
            return new StitchingEntityData(getEntityDtoBuilder(), targetId, IdentityGenerator.next(), 0);
        }
    }

    @Nonnull
    public static TopologyStitchingGraph newStitchingGraph(
        @Nonnull final Map<String, StitchingEntityData> topologyMap) {

        final TopologyStitchingGraph graph = new TopologyStitchingGraph(topologyMap.size());
        topologyMap.values().forEach(entity -> graph.addStitchingData(entity, topologyMap));

        return graph;
    }

    /**
     * A matcher that allows asserting that a particular entity in the topology is acting as a provider
     * for exactly a certain number of entities.
     */

    /**
     * A matcher that allows asserting that a particular entity in the topology is acting as a provider
     * for exactly a certain number of entities.
     */
    public static Matcher<StitchingEntity> isBuyingCommodityFrom(final String providerOid) {
        return new BaseMatcher<StitchingEntity>() {
            @Override
            @SuppressWarnings("unchecked")
            public boolean matches(Object o) {
                final StitchingEntity entity = (StitchingEntity) o;
                for (StitchingEntity provider : entity.getCommoditiesBoughtByProvider().keySet()) {
                    if (providerOid.equals(provider.getLocalId())) {
                        return true;
                    }
                }

                return false;
            }
            @Override
            public void describeTo(Description description) {
                description.appendText("Entity should be buying a commodity from provider with oid " +
                    providerOid);
            }
        };
    }
}
