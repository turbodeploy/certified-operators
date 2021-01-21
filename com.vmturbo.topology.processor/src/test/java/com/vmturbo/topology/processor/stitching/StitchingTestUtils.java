package com.vmturbo.topology.processor.stitching;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
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
        return stitchingData(localId, entityType,
            toArray(providerIds.stream()
                .map(providerId -> new CommodityBoughtOutline(providerId, CommodityType.CPU))
                .collect(Collectors.toList())));
    }

    private static CommodityBoughtOutline[] toArray(@Nonnull final Collection<CommodityBoughtOutline> outlines) {
        final CommodityBoughtOutline[] outlinesArray = new CommodityBoughtOutline[outlines.size()];
        return outlines.toArray(outlinesArray);
    }

    @Nonnull
    public static StitchingDataAllowingTargetChange
    stitchingData(@Nonnull final String localId,
                  @Nonnull final EntityType entityType,
                  @Nonnull final CommodityBoughtOutline... commoditiesBought) {
        final EntityDTO.Builder builder = EntityDTO.newBuilder()
            .setId(localId)
            .setEntityType(entityType);

        final Map<String, List<CommodityBoughtOutline>> boughtMap = Arrays.stream(commoditiesBought)
            .collect(Collectors.groupingBy(outline -> outline.providerId));

        boughtMap.forEach((providerId, outlines) -> builder.addCommoditiesBought(CommodityBought.newBuilder()
            .setProviderId(providerId)
            .addAllBought(outlines.stream()
                .map(outline -> CommodityDTO.newBuilder().setCommodityType(outline.commodityType).build())
                .collect(Collectors.toList()))));

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
            super(entityDtoBuilder, targetId, oid, 0, true);
        }

        public StitchingEntityData forTarget(final long targetId) {
            return StitchingEntityData.newBuilder(getEntityDtoBuilder())
                    .targetId(targetId)
                    .oid(IdentityGenerator.next())
                    .lastUpdatedTime(0).build();
        }
    }

    @Nonnull
    public static TopologyStitchingGraph newStitchingGraph(
        @Nonnull final Map<String, StitchingEntityData> topologyMap) {

        final TopologyStitchingGraph graph = new TopologyStitchingGraph(topologyMap.size());
        topologyMap.values().forEach(entity -> graph.addStitchingData(entity, topologyMap));

        return graph;
    }

    public static void visitNeighbors(@Nonnull final StitchingEntity start,
                                      @Nonnull final Set<StitchingEntity> visited,
                                      @Nonnull final Set<EntityType> entityBounds,
                                      @Nonnull final Function<StitchingEntity, Collection<StitchingEntity>> neighborFunction) {
        visited.add(start);

        if (!entityBounds.contains(start.getEntityType())) {
            neighborFunction.apply(start).forEach(neighbor -> {
                if (!visited.contains(neighbor)) {
                    visitNeighbors(neighbor, visited, entityBounds, neighborFunction);
                }
            });
        }
    }

    public static class CommodityBoughtOutline {
        public final CommodityType commodityType;
        public final String providerId;

        public CommodityBoughtOutline(@Nonnull final String providerId,
                                      @Nonnull final CommodityType commodityType) {
            this.commodityType = Objects.requireNonNull(commodityType);
            this.providerId = Objects.requireNonNull(providerId);
        }
    }

    public static CommodityBoughtOutline buying(@Nonnull final CommodityType commodityType,
                                                @Nonnull final String providerId) {
        return new CommodityBoughtOutline(providerId, commodityType);
    }

    public static TopologyDTO.CommodityType commodityType(@Nonnull final CommodityDTO.CommodityType commodityType) {
        return TopologyDTO.CommodityType.newBuilder()
            .setType(commodityType.getNumber())
            .build();
    }

    public static TopologyDTO.CommodityType commodityType(@Nonnull final CommodityDTO.CommodityType commodityType,
                                                    @Nonnull final String key) {
        return TopologyDTO.CommodityType.newBuilder()
            .setKey(key)
            .setType(commodityType.getNumber())
            .build();
    }

    /**
     * Read entities from a file. Can read entities from a zip file or a flat file.
     * Entities are expected to be SDK DTO protobufs after being serialized to JSON.
     *
     * @param callingClass The class requesting the entities to be read.
     * @param fileName The name of the file containing the entities.
     * @param startingOid The first OID to assign the first entity read from the file.
     *                    Entities read from the file are assigned OIDs in monotonically increasing order.
     * @return
     * @throws Exception
     */
    public static Map<Long, EntityDTO> sdkDtosFromFile(@Nonnull final Class callingClass,
                                                       @Nonnull final String fileName,
                                                       long startingOid) throws Exception {
        final URL url = callingClass.getClassLoader().getResources(fileName).nextElement();
        String discovery;

        if (fileName.endsWith("zip")) {
            ZipInputStream zis = new ZipInputStream(url.openStream());
            final ZipEntry zipEntry = zis.getNextEntry();
            final byte[] bytes = new byte[(int) zipEntry.getSize()];
            int read = 0;
            while ((read += zis.read(bytes, read, bytes.length - read)) < bytes.length) { }
            discovery = new String(bytes, "UTF-8");
        } else {
            StringWriter writer = new StringWriter();
            IOUtils.copy(url.openStream(), writer, "UTF-8");
            discovery = writer.toString();
        }

        final List<EntityDTO> entities = Arrays.asList(
            ComponentGsonFactory.createGson().fromJson(discovery, EntityDTO[].class));
        AtomicLong nextOid = new AtomicLong(startingOid);
        return entities.stream()
            .collect(Collectors.toMap(entity -> nextOid.getAndIncrement(), Function.identity()));
    }

    public static void writeSdkDtosToFile(@Nonnull final String fileName,
                                          @Nonnull final Collection<StitchingEntity> entities) throws Exception {
        Path file = Paths.get(fileName);
        PrintWriter printWriter = new PrintWriter(file.toFile());

        final Collection<EntityDTO> sdkDtos = entities.stream()
            .map(e -> {
                final EntityDTO.Builder builder = e.getEntityBuilder();
                builder.addAllCommoditiesSold(e.getCommoditiesSold()
                    .map(Builder::build)
                    .collect(Collectors.toList()));

                e.getCommodityBoughtListByProvider().forEach((provider, commodityBoughtList) -> {
                    commodityBoughtList.forEach(commodityBought -> {
                        final CommodityBought.Builder boughtBuilder = CommodityBought.newBuilder()
                                .setProviderId(provider.getLocalId())
                                .addAllBought(commodityBought.getBoughtList().stream()
                                        .map(CommodityDTO.Builder::build)
                                        .collect(Collectors.toList()));
                        builder.addCommoditiesBought(boughtBuilder);
                    });
                });

                return builder.build();
            }).collect(Collectors.toList());

        printWriter.write(ComponentGsonFactory.createGson().toJson(sdkDtos.toArray()));
        System.out.println("Wrote " + entities.size() + " entities to file " + file.toAbsolutePath());
        printWriter.flush();
        printWriter.close();
    }

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
                for (StitchingEntity provider : entity.getCommodityBoughtListByProvider().keySet()) {
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

    /**
     * A matcher that allows asserting that a particular entity in the topology is acting as a provider
     * for exactly a certain number of entities.
     *
     * Ignores origin in comparison.
     */
    public static Matcher<TopologyEntityDTO> matchesEntityIgnoringOrigin(final TopologyEntityDTO secondEntity) {
        return new BaseMatcher<TopologyEntityDTO>() {
            @Override
            @SuppressWarnings("unchecked")
            public boolean matches(Object o) {
                final TopologyEntityDTO firstEntity = (TopologyEntityDTO) o;
                final TopologyEntityDTO firstEntityWithoutBought = firstEntity.toBuilder()
                    .clearCommoditiesBoughtFromProviders()
                    // TODO: This code will be removed, and numDisks commodity created in plan when VVs are ready (OM-59261)
                    .clearCommoditySoldList()
                    .clearOrigin()
                    .build();
                final TopologyEntityDTO secondEntityWithoutBought = secondEntity.toBuilder()
                    .clearCommoditiesBoughtFromProviders()
                    // TODO: This code will be removed, and numDisks commodity created in plan when VVs are ready (OM-59261)
                    .clearCommoditySoldList()
                    .clearOrigin()
                    .build();


                // TODO: This code will be removed, and numDisks commodity created in plan when VVs are ready (OM-59261)
                final Set<CommoditySoldDTO> firstSoldCommodities = firstEntity.getCommoditySoldListList().stream()
                        .filter(x -> CommodityType.NUM_DISK.equals(x.getCommodityType()))
                        .collect(Collectors.toSet());
                final Set<CommoditySoldDTO> secondSoldCommodities = secondEntity.getCommoditySoldListList().stream()
                        .filter(x -> CommodityType.NUM_DISK.equals(x.getCommodityType()))
                        .collect(Collectors.toSet());
                if (!firstSoldCommodities.equals(secondSoldCommodities)) {
                    return false;
                }

                // Compare ignoring order
                final Set<CommoditiesBoughtFromProvider> firstCommodities =
                    firstEntity.getCommoditiesBoughtFromProvidersList().stream()
                        .collect(Collectors.toSet());
                final Set<CommoditiesBoughtFromProvider> secondCommodities =
                    secondEntity.getCommoditiesBoughtFromProvidersList().stream()
                        .collect(Collectors.toSet());

                return firstCommodities.equals(secondCommodities);
            }
            @Override
            public void describeTo(Description description) {
                description.appendText("Entities should match");
            }
        };
    }
}
