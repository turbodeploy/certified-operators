package com.vmturbo.mediation.udt.explore.collectors;

import static com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.mediation.udt.explore.DataProvider;
import com.vmturbo.mediation.udt.inventory.UdtChildEntity;
import com.vmturbo.mediation.udt.inventory.UdtEntity;
import com.vmturbo.mediation.util.HashGenerator;

/**
 * The collector for automated topology data definition.
 */
public class AutomatedDefinitionCollector extends UdtCollector<AutomatedEntityDefinition> {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Constructor.
     *
     * @param definitionId - ID of topology data definition.
     * @param definition   - topology data definition instance.
     */
    @ParametersAreNonnullByDefault
    public AutomatedDefinitionCollector(Long definitionId, AutomatedEntityDefinition definition) {
        super(definitionId, definition);
    }

    /**
     * A method for populating UDT entities.
     * 1) Retrieve topology entities that have a TAG defined in {@link AutomatedEntityDefinition};
     * 2) Process them to create a map:
     *    KEY: tag value
     *    VALUE: collection of entities
     * 3) Convert this map to {@link UdtEntity} set:
     *    - one tag value is for one 'UdtEntity'
     *    - the name of created 'UdtEntity': $prefix_$tag_$tagValue
     *    - topology entities with this tag value are children of created 'UdtEntity'
     *
     * @param dataProvider - an instance of {@link DataProvider}.
     * @return a set of UDT entities.
     */
    @Nonnull
    @Override
    public Set<UdtEntity> collectEntities(@Nonnull DataProvider dataProvider) {
        if (!isValidDefinition()) {
            LOGGER.error("Topology definition is incorrect: {}", definition);
            return Collections.emptySet();
        }
        Set<TopologyEntityDTO> topologyEntities = retrieveTopologyEntities(dataProvider);
        Multimap<String, TopologyEntityDTO> tagValueToTaggedEntities = getTagValuesMap(topologyEntities);
        return tagValueToTaggedEntities .asMap().entrySet().stream()
                .map(entry -> createUdtEntity(entry.getKey(), entry.getValue()))
                .collect(Collectors.toSet());
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    private UdtEntity createUdtEntity(String tagValue, Collection<TopologyEntityDTO> children) {
        String tagKey = definition.getTagGrouping().getTagKey();
        Set<UdtChildEntity> childEntities = createChildren(children);
        String udtEntityName = String.format("%s_%s_%s", definition.getNamingPrefix(), tagKey, tagValue);
        String udtEntityId = generateUdtEntityId(udtEntityName);
        return new UdtEntity(definition.getEntityType(), udtEntityId, udtEntityName, childEntities);
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    private Set<TopologyEntityDTO> retrieveTopologyEntities(DataProvider dataProvider) {
        String tagKey = definition.getTagGrouping().getTagKey();
        return dataProvider.getEntitiesByTag(tagKey, definition.getConnectedEntityType());
    }

    @Nonnull
    private Set<UdtChildEntity> createChildren(@Nonnull Collection<TopologyEntityDTO> children) {
        return children.stream().map(e -> new UdtChildEntity(e.getOid(), definition.getConnectedEntityType()))
                .collect(Collectors.toSet());
    }

    @Nonnull
    private Multimap<String, TopologyEntityDTO> getTagValuesMap(@Nonnull Set<TopologyEntityDTO> entities) {
        Multimap<String, TopologyEntityDTO> map = ArrayListMultimap.create();
        for (TopologyEntityDTO entity : entities) {
            String tagValue = getTagValue(entity);
            if (tagValue != null) {
                map.put(tagValue, entity);
            }
        }
        return map;
    }

    @Nullable
    private String getTagValue(@Nonnull TopologyEntityDTO entity) {
        String tagKey = definition.getTagGrouping().getTagKey();
        Tags entityTags = entity.getTags();
        if (entityTags.containsTags(tagKey)) {
            TagValuesDTO valuesDto = entityTags.getTagsMap().get(tagKey);
            ArrayList<String> values = new ArrayList<>(valuesDto.getValuesList());
            if (!values.isEmpty()) {
                return values.get(0);
            }
        }
        return null;
    }

    @Nonnull
    @VisibleForTesting
    @ParametersAreNonnullByDefault
    String generateUdtEntityId(@Nonnull String udtEntityName) {
        return HashGenerator.hashSHA1(String.valueOf(definitionId), udtEntityName);
    }

    private boolean isValidDefinition() {
        return definition.hasNamingPrefix() && definition.hasEntityType()
                && definition.hasConnectedEntityType()
                && definition.hasTagGrouping() && definition.getTagGrouping().hasTagKey();
    }
}
