package com.vmturbo.extractor.docgen;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base class for doc section generators.
 *
 * <p>Sections can be independently generated, and each gives rise to a single XML file from
 * which documents may be created via xslt or other transformation methods.</p>
 *
 * <p>Each section will document a collection of "items" that are associated in some way, e.g. all
 * the columns in a table definition, or all the members of an enumeration type. The bulk of the
 * generated XML will be an `&lt;Items&gt;` element containing a sequence of `&lt;Item&gt;`
 * elements, each documenting one of the underlying items.</p>
 *
 * @param <T> type of items that will appear in the section.
 */
public abstract class Section<T> {
    private final Logger logger = LogManager.getLogger(getClass());

    protected final JsonPointer docPathPrefix;
    private final JsonNode docTree;
    private final String name;

    /**
     * Create a new instance.
     *
     * @param name          Section name, included as attribute in generated XML root element
     * @param docPathPrefix JSONPointer string that identifies a sub-tree of the doc tree that will
     *                      be used to obtain doc snippets for use in this section
     * @param docTree       JSON structure containing doc snippets to be included in the docs
     */
    public Section(final String name, final String docPathPrefix, JsonNode docTree) {
        this.name = name;
        this.docPathPrefix = JsonPointer.compile(docPathPrefix);
        this.docTree = docTree;
    }

    /**
     * Get a name for this section, which will appear in the root element of the generated XML.
     *
     * @return section name
     */
    public String getName() {
        return name;
    }

    /**
     * Obtain a doc snippet that should appear in an "introduction" section of the generated doc.
     *
     * <p>By default, this is obtained from the doc-tree.</p>
     *
     * @return doc snippet to be used for this section's introduction
     */
    public Optional<String> getIntroText() {
        final JsonPointer path = docPathPrefix.append(JsonPointer.compile("/intro"));
        return getDocText(path);
    }

    /**
     * Get the items to be included in this section, in the order they should be rendered.
     *
     * @return item list
     */
    public abstract List<T> getItems();

    /**
     * Get the full path, within the doc-tree, for doc snippets that should be used for this item.
     *
     * <p>Individual doc snippets, generally corresponding to individual per-item fields, appear
     * as values of child properties.</p>
     *
     * <p>Normally, this should be a different value for each item, but that's not required.
     * If not, then the same doc snippet will be used for multiple items.</p>
     *
     * @param item the item whose snippets are needed
     * @return the full JSONPointer to access the snippets
     */
    public abstract JsonPointer getItemDocPath(T item);

    /**
     * Get the list of fields that will be provided for each item. Fields are provided as
     * sub-elements of generated `&lt;Item&gt;` element.
     *
     * @param item the item whose field names to get
     * @return list of field names
     */
    public abstract List<String> getFieldNames(T item);

    /**
     * Add the value for a given field of the given item. The implementation here will attempt to
     * obtain a snippet from the doc-tree, but overriding classes often provide computed values for
     * some fields.
     *
     * @param item      item whose field value is needed
     * @param fieldName name of field to retrieve
     */
    public void addItemFieldValue(T item, String fieldName) {
        final JsonPointer path = getItemDocPath(item).append(JsonPointer.compile("/" + fieldName));
        final JsonNode valueNode = getItemFieldValue(item, fieldName);
        addValueNode(path, valueNode);
    }

    /**
     * Get the value for a given field of the given item. The implementation here will attempt to
     * obtain a snippet from the doc-tree, but overriding classes often provide computed values for
     * some fields.
     *
     * @param item      item whose field value is needed
     * @param fieldName name of field to retrieve
     * @return the field value, or null if none can be found
     */
    public abstract JsonNode getItemFieldValue(T item, String fieldName);

    /**
     * Obtain a doc snippet from the doc-tree.
     *
     * <p>When the snippet is found to be missing, it is replaced in the doc-tree with a null value
     * at the specified location, so that if the tree-rewriting is enabled, these null entries will
     * show up in the rewritten doc-tree, minimizing the risk of structural errors or mis-spelled
     * properties when manually editing the do-tree.</p>
     *
     * @param path JSON pointer to locate the snippet in the doc tree.
     * @return the located snippet, or null if not present.
     */
    @VisibleForTesting
    Optional<String> getDocText(final JsonPointer path) {
        final JsonNode value = docTree.at(path);
        if (value.isTextual()) {
            return Optional.of(value.asText());
        } else if (value.isMissingNode() || value.isNull()) {
            // if node doesn't exist or has null value, we're missing as doc snippet
            logger.warn("Missing doc value at {}", path);
            if (value.isMissingNode()) {
                // add a null node it expected node is missing, in case --rewrite-tree is active
                try {
                    addNodeAt(docTree, path, JsonNodeFactory.instance.nullNode());
                } catch (ClassCastException e) {
                    logger.error("Current doc-tree is incompatible with required path {}", path);
                }
            }
            return Optional.empty();
        } else {
            logger.warn("Non-text doc value at {}", path);
            return Optional.empty();
        }
    }

    void addValueNode(final JsonPointer path, @Nullable final JsonNode valueNode) {
        final JsonNode value = docTree.at(path);
        if (value.isMissingNode()) {
            // add a null node it expected node is missing, in case --rewrite-tree is active
            try {
                addNodeAt(docTree, path, Optional.ofNullable(valueNode).orElse(JsonNodeFactory.instance.nullNode()));
                logger.info("Added new section: {}", path);
            } catch (ClassCastException e) {
                logger.error("Current doc-tree is incompatible with required path {}", path);
            }
        }
    }

    /**
     * Utility to add a node into a JSON structure at a specified location. Any required ancestor
     * nodes are created as a side-effect.
     *
     * <p>An exception will be thrown if the current JSON structure is incompatible with the given
     * JSON pointer.</p>
     *
     * @param tree JSON structure into which the node should be injected
     * @param path JSON pointer that indicates where the node should reside after injection
     * @param node node to be injected
     * @throws ClassCastException if the existing JSON structure is incompatible with the operation
     */
    @VisibleForTesting
    static void addNodeAt(JsonNode tree, JsonPointer path, JsonNode node) throws ClassCastException {
        // see if we have a container for the missing tail
        final JsonPointer head = path.head();
        if (tree.at(head).isMissingNode()) {
            // no, create either an array or object as needed for the container
            JsonPointer last = path.last();
            JsonNode container;
            if (last.mayMatchElement()) {
                container = JsonNodeFactory.instance.arrayNode();
                ((ArrayNode)container).set(last.getMatchingIndex(), node);
            } else {
                container = JsonNodeFactory.instance.objectNode();
                ((ObjectNode)container).set(last.getMatchingProperty(), node);
            }
            addNodeAt(tree, head, container);
        } else {
            // we have a container - add new structure, assuming container is compatible
            // if not, we'll throw ClassCastException that the caller should catch
            JsonNode container = tree.at(head);
            final JsonPointer last = path.last();
            if (last.mayMatchElement()) {
                ((ArrayNode)container).set(last.getMatchingIndex(), node);
            } else {
                ((ObjectNode)container).set(last.getMatchingProperty(), node);
            }
        }
    }
}
