package com.vmturbo.mediation.udt.explore.collectors;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition;
import com.vmturbo.common.protobuf.search.Search.TaggedEntities;
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
     * 1) Retrieve a map based on a tag key:
     *    KEY:    tag value
     *    VALUE:  collection of entities OIDs.
     * 2) Convert this map to {@link UdtEntity} set:
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
        final String tagKey = definition.getTagGrouping().getTagKey();
        final Map<String, TaggedEntities> valuesToOidsMap
                = dataProvider.retrieveTagValues(tagKey, definition.getConnectedEntityType());
        LOGGER.info("For tag {} retrieved {} values.", tagKey, valuesToOidsMap.size());
        return valuesToOidsMap.entrySet().stream()
                .map(entry -> createUdtEntity(entry.getKey(), entry.getValue().getOidList()))
                .collect(Collectors.toSet());
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    private UdtEntity createUdtEntity(String tagValue, List<Long> childrenOids) {
        String tagKey = definition.getTagGrouping().getTagKey();
        Set<UdtChildEntity> childEntities = createChildren(childrenOids);
        String udtEntityName = String.format("%s_%s_%s", definition.getNamingPrefix(), tagKey, tagValue);
        String udtEntityId = generateUdtEntityId(udtEntityName);
        return new UdtEntity(definition.getEntityType(), udtEntityId, udtEntityName, childEntities);
    }

    @Nonnull
    private Set<UdtChildEntity> createChildren(@Nonnull List<Long> childrenOids) {
        Set<UdtChildEntity> childEntities = Sets.newHashSet();
        for (Long oid : childrenOids) {
            childEntities.add(new UdtChildEntity(oid, definition.getConnectedEntityType()));
        }
        return childEntities;
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
