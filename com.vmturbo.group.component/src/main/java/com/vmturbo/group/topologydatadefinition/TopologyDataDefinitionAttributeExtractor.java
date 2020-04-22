package com.vmturbo.group.topologydatadefinition;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.identity.attributes.AttributeExtractor;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.attributes.SimpleMatchingAttributes;

/**
 * Class to extract identifying attributes from TopologyDataDefinition for use in generating an OID.
 */
public class TopologyDataDefinitionAttributeExtractor implements
    AttributeExtractor<TopologyDataDefinition> {

    private static final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    static final String ENTITY_TYPE_ATTRIBUTE = "EntityType";

    @VisibleForTesting
    static final String NAME_ATTRIBUTE = "Name";

    static final String DEFINITION_TYPE_ATTRIBUTE = "DefinitionType";

    @VisibleForTesting
    static final String MANUAL_DEFINITION = "ManualDefinition";

    @VisibleForTesting
    static final String AUTOMATED_DEFINITION = "AutomatedDefinition";

    @Nonnull
    @Override
    public IdentityMatchingAttributes extractAttributes(
            @Nonnull final TopologyDataDefinition topologyDataDefinition) {
        final SimpleMatchingAttributes.Builder simpleMatchingAttributes =
            SimpleMatchingAttributes.newBuilder();
        if (topologyDataDefinition.hasManualEntityDefinition()) {
            simpleMatchingAttributes.addAttribute(DEFINITION_TYPE_ATTRIBUTE, MANUAL_DEFINITION);
            simpleMatchingAttributes.addAttribute(ENTITY_TYPE_ATTRIBUTE,
                topologyDataDefinition.getManualEntityDefinition().getEntityType().name());
            simpleMatchingAttributes.addAttribute(NAME_ATTRIBUTE,
                topologyDataDefinition.getManualEntityDefinition().getEntityName());
        } else {
            simpleMatchingAttributes.addAttribute(DEFINITION_TYPE_ATTRIBUTE, AUTOMATED_DEFINITION);
            simpleMatchingAttributes.addAttribute(ENTITY_TYPE_ATTRIBUTE,
                topologyDataDefinition.getAutomatedEntityDefinition().getEntityType().name());
            simpleMatchingAttributes.addAttribute(NAME_ATTRIBUTE,
                topologyDataDefinition.getAutomatedEntityDefinition().getNamingPrefix()
                    + topologyDataDefinition.getAutomatedEntityDefinition()
                        .getTagGrouping().getTagKey());
        }
        logger.debug("Generated matching attributes for entity definition: {}",
            () -> simpleMatchingAttributes.build().getMatchingAttributes());
        return simpleMatchingAttributes.build();
    }
}
