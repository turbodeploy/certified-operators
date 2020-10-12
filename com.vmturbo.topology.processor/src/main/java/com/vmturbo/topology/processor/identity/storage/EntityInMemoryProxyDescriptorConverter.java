package com.vmturbo.topology.processor.identity.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;

import com.vmturbo.topology.processor.identity.PropertyDescriptor;

/**
 * A JOOQ converter that saves {@link EntityInMemoryProxyDescriptor}s as JSON strings.
 * <p>
 * In the future, it may be worthwhile to separate the object stored to the database
 * and the object that the {@link IdentityServiceInMemoryUnderlyingStore} works with, but
 * currently the {@link EntityInMemoryProxyDescriptor} is a simple-enough and infrequently-changing
 * object.
 */
public class EntityInMemoryProxyDescriptorConverter implements Converter<String, EntityInMemoryProxyDescriptor> {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Jackson mapper to read and write JSON.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * The current key for the OID field of the JSON object that represents the descriptor
     * in the database.
     *
     * If changing this name, put the previous name in PAST_OID_PROP_NAMES to allow parsing
     * of old entries.
     */
    private static final String CUR_OID_PROP_NAME = "oid";

    /**
     * The current key for the identifying properties field of the JSON object that represents
     * the descriptor in the database.
     *
     * If changing this name, put the previous name in PAST_ID_PROPS_PROP_NAMES to allow parsing
     * of old entries.
     */
    @VisibleForTesting
    static final String CUR_ID_PROPS_PROP_NAME = "idProps";

    /**
     * The current key for the heuristic properties field of the JSON object that represents
     * the descriptor in the database.
     *
     * If changing this name, put the previous name in PAST_HEURISTIC_PROPS_PROP_NAMES to allow
     * parsing of old entries.
     */
    private static final String CUR_HEURISTIC_PROPS_PROP_NAME = "heuristicProps";

    private static final List<String> PAST_OID_PROP_NAMES = ImmutableList.of();

    @VisibleForTesting
    static final List<String> PAST_ID_PROPS_PROP_NAMES = ImmutableList.of("identifyingProperties");

    private static final List<String> PAST_HEURISTIC_PROPS_PROP_NAMES =
            ImmutableList.of("heuristicProperties");

    @Override
    public EntityInMemoryProxyDescriptor from(final String databaseObject) {
        try {
            final JsonNode root = MAPPER.reader().readTree(databaseObject);
            Optional<JsonNode> oidNode = getChildNode(root,
                    CUR_OID_PROP_NAME, PAST_OID_PROP_NAMES);
            Optional<JsonNode> idNodeOpt = getChildNode(root,
                    CUR_ID_PROPS_PROP_NAME, PAST_ID_PROPS_PROP_NAMES);
            Optional<JsonNode> heuristicNodeOpt = getChildNode(root,
                    CUR_HEURISTIC_PROPS_PROP_NAME, PAST_HEURISTIC_PROPS_PROP_NAMES);

            if (!oidNode.isPresent() || !idNodeOpt.isPresent() || !heuristicNodeOpt.isPresent()) {
                LOGGER.error("Failed to convert properties text - " +
                        "not all nodes are present in the JSON: {}", databaseObject);
                return null;
            }

            final long oid = oidNode.get().asLong();
            final JsonNode idNode = idNodeOpt.get();
            final JsonNode heuristicNode = heuristicNodeOpt.get();

            final List<PropertyDescriptor> idProps =
                    new ArrayList<>(idNode.size());
            for (int i = 0; i < idNode.size(); ++i) {
                final String idProp = idNode.get(i).asText();
                idProps.add(IdentityServiceInMemoryUnderlyingStore.parseString(idProp));
            }

            final List<PropertyDescriptor> heuristicProps =
                    new ArrayList<>(heuristicNode.size());
            for (int i = 0; i < heuristicNode.size(); ++i) {
                final String heuristicProp = heuristicNode.get(i).asText();
                heuristicProps.add(IdentityServiceInMemoryUnderlyingStore.parseString(heuristicProp));
            }

            return new EntityInMemoryProxyDescriptor(oid, idProps, heuristicProps);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public String to(final EntityInMemoryProxyDescriptor userObject) {
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put(CUR_OID_PROP_NAME, userObject.getOID());


        final ArrayNode heuristicNode = objectNode.putArray(CUR_HEURISTIC_PROPS_PROP_NAME);
        userObject.getHeuristicProperties().stream()
                .map(IdentityServiceInMemoryUnderlyingStore::propertyAsString)
                .forEach(heuristicNode::add);

        final ArrayNode idProps = objectNode.putArray(CUR_ID_PROPS_PROP_NAME);
        userObject.getIdentifyingProperties().stream()
                .map(IdentityServiceInMemoryUnderlyingStore::propertyAsString)
                .forEach(idProps::add);

        return objectNode.toString();
    }

    @Override
    public Class<String> fromType() {
        return String.class;
    }

    @Override
    public Class<EntityInMemoryProxyDescriptor> toType() {
        return EntityInMemoryProxyDescriptor.class;
    }

    /**
     * Get the child node of a root node.
     *
     * @param root The root node.
     * @param propName The name of the child.
     * @param alternativeNames If propName is not found in the root, alternative names to search
     *                         for.
     * @return The child node identified by propName, if it exists. Otherwise, the first existing
     *         child node identified by a name in alternativeNames. An empty optional if there is
     *         absolutely no match.
     */
    private Optional<JsonNode> getChildNode(@Nonnull final JsonNode root,
                                            @Nonnull final String propName,
                                            @Nonnull final List<String> alternativeNames) {
        JsonNode node = root.get(propName);
        if (node == null) {
            for (String otherName : alternativeNames) {
                node = root.get(otherName);
                if (node != null) {
                    break;
                }
            }
        }
        return Optional.ofNullable(node);
    }
}
