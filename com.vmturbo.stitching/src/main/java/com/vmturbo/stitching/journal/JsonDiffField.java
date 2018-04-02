package com.vmturbo.stitching.journal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flipkart.zjsonpatch.JsonPatch;
import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.journal.JsonDiffDescription.OperationType;

public class JsonDiffField {
    /**
     * Differences are listed in ranked order (from low to high). If an object is affected by multiple modifications,
     * we mark the object only with the highest-ranked difference type. A difference with a higher rank
     * may overwrite a difference with lower rank but not vice-versa.
     */
    public enum DifferenceType {
        UNMODIFIED,
        CHILD_MODIFIED,
        ADDITION,
        REMOVAL,
        REPLACEMENT,
    }

    private String fieldName;
    private final JsonDiffField parent;

    protected DifferenceType differenceType = DifferenceType.UNMODIFIED;
    private String originalValue;
    private String replacementValue;

    /**
     * These fields are retained for all verbosities of LOCAL_CONTEXT or higher.
     */
    private static final Set<String> contextFieldsToRetain = new HashSet<>();
    static {
        contextFieldsToRetain.add("entityType");
        contextFieldsToRetain.add("used");
        contextFieldsToRetain.add("capacity");
        contextFieldsToRetain.add("oid");
        contextFieldsToRetain.add("targetId");
        contextFieldsToRetain.add("providerId");
    }

    /**
     * These fields are always retained for all repositories.
     */
    private static final Set<String> requiredFieldsToRetain = new HashSet<>();
    static {
        requiredFieldsToRetain.add("key");
        requiredFieldsToRetain.add("type");
        requiredFieldsToRetain.add("commodityType");
    }

    public static JsonDiffField newField(@Nonnull final JsonNode associatedNode) {
        final JsonDiffField field;

        if (associatedNode.isArray()) {
            field = new JsonDiffArray();
        } else if (associatedNode.isObject()) {
            field = new JsonDiffObject();
        } else {
            field = new JsonDiffField();
        }

        field.originalValue = associatedNode.toString();
        return field;
    }

    public static JsonDiffField newField(@Nonnull final JsonNode associatedNode,
                                         @Nonnull final String fieldName,
                                         @Nonnull final JsonDiffField parent) {
        final JsonDiffField field;

        if (associatedNode.isArray()) {
            field = new JsonDiffArray(fieldName, parent);
        } else if (associatedNode.isObject()) {
            field = new JsonDiffObject(fieldName, parent);
        } else {
            field = new JsonDiffField(fieldName, parent);
        }

        field.originalValue = associatedNode.toString();
        return field;
    }

    public JsonDiffField() {
        this.fieldName = null;
        this.parent = null;
    }

    public JsonDiffField(@Nonnull final String fieldName,
                         @Nonnull final JsonDiffField parentField) {
        this.parent = Objects.requireNonNull(parentField);
        this.fieldName = Objects.requireNonNull(fieldName);
    }

    public DifferenceType getDifferenceType() {
        return differenceType;
    }

    public void setDifferenceType(DifferenceType differenceType) {
        if (differenceType.ordinal() > this.differenceType.ordinal()) {
            this.differenceType = differenceType;
        }
    }

    public boolean hasChanges() {
        return differenceType != DifferenceType.UNMODIFIED;
    }

    public JsonDiffField getParent() {
        return parent;
    }

    public String pathString() {
        return constructPath().stream().collect(Collectors.joining("/", "/", ""));
    }

    public Deque<String> constructPath() {
        final Deque<String> path = new ArrayDeque<>();
        addToPath(path);

        return path;
    }

    public void addChildren(@Nonnull final JsonNode associatedNode) {
        associatedNode.fields().forEachRemaining(entry -> {
            addChild(entry.getKey(), entry.getValue());
        });
    }

    public boolean factorDifference(@Nonnull final Deque<String> path,
                                    @Nonnull final RootJsonNode rootNode,
                                    @Nonnull final JsonDiffDescription diffDescription) {
        setDifferenceType(associatedDifferenceType(diffDescription.getOp()));

        switch (associatedDifferenceType(diffDescription.getOp())) {
            case REPLACEMENT:
                replacementValue = diffDescription.getValue().toString();
                break;
        }

        return true;
    }

    protected void addChild(@Nonnull final String fieldName,
                         @Nonnull final JsonNode associatedNode) {
        throw new IllegalStateException("JsonField does not support adding children");
    }

    public void renderToBuilder(@Nonnull final StringBuilder stringBuilder,
                                @Nonnull final JsonNode node,
                                @Nonnull final SpaceRenderer spaces,
                                @Nonnull final Verbosity verbosity) throws JsonProcessingException {
        if (verbosity == Verbosity.PREAMBLE_ONLY_VERBOSITY) {
            // Don't render any changes to the builder.
            return;
        }

        stringBuilder.append(spaces.render());
        renderDifference(node, null, stringBuilder, spaces, verbosity);
    }

    private void renderToBuilder(@Nonnull final String fieldName,
                                @Nonnull final StringBuilder stringBuilder,
                                @Nonnull final JsonNode node,
                                @Nonnull final SpaceRenderer spaces,
                                @Nonnull final Verbosity verbosity) throws JsonProcessingException {
        stringBuilder.append(spaces.render());
        stringBuilder.append("\"").append(fieldName).append("\":").append(spaces.space());

        renderDifference(node, fieldName, stringBuilder, spaces, verbosity);
    }

    protected void renderDifference(@Nonnull final JsonNode node,
                                    @Nullable final String fieldName,
                                    @Nonnull final StringBuilder stringBuilder,
                                    @Nonnull final SpaceRenderer spaces,
                                    @Nonnull final Verbosity verbosity) throws JsonProcessingException {
        switch (differenceType) {
            case UNMODIFIED:
                stringBuilder.append(semanticTranslation(fieldName, originalValue, spaces));
                return;
            case REPLACEMENT:
                stringBuilder
                    .append("((")
                    .append(originalValue)
                    .append(spaces.space()).append("-->").append(spaces.space())
                    .append(replacementValue)
                    .append("))");
                return;
            case REMOVAL:
                stringBuilder.append(semanticTranslation(fieldName, originalValue, spaces));
                return;
            case ADDITION:
                stringBuilder.append(semanticTranslation(fieldName, originalValue, spaces));
                return;
            case CHILD_MODIFIED:
                throw new IllegalArgumentException("Child modified changes cannot be applied to terminal node " + node);
        }
    }

    /**
     * Some fields that are never modified, we want to translate into more
     * semantically meaningful representation (ie TopologyEntityDTO entityTYpe is a
     * meaningless number when we actually want to see the equivalent EntityType enum String).
     *
     * Most fields are not translated.
     *
     * @param fieldName The name of the field to be translated.
     * @param value The value of the field to be translated.
     * @param spaces A renderer capable of rendering spaces in a given format.
     * @return The translated field value.
     */
    private String semanticTranslation(@Nullable final String fieldName,
                                       @Nonnull final String value,
                                       @Nonnull final SpaceRenderer spaces) {
        if (fieldName != null) {
            if (fieldName.equals("entityType")) { // TopologyEntityDTO EntityType
                if (StringUtils.isNumeric(value)) {
                    final int entityType = Integer.parseInt(value);
                    return value + spaces.space() + "[[" + EntityType.forNumber(entityType) + "]]";
                }
            } else if (fieldName.equals("type")) { // TopologyEntityDTO CommodityType.type
                // Note that this could potentially mis-translate some fields labelled "type"
                // that are not actually commodityTypes but it seems to do the right thing for now.
                if (StringUtils.isNumeric(value)) {
                    final int entityType = Integer.parseInt(value);
                    return value + spaces.space() + "[[" + CommodityType.forNumber(entityType) + "]]";
                }
            }
        }

        // If we didn't hit one of the above special cases, just return the original value.
        return value;
    }

    void renderAdditional(final int numPrinted, final boolean anyPrinted, final int total,
                          @Nonnull final StringBuilder stringBuilder, @Nonnull final SpaceRenderer spaces) {
        boolean anyUnchanged = numPrinted < total;
        if (anyPrinted) {
            if (anyUnchanged) {
                stringBuilder.append(",");
            }
            stringBuilder.append(spaces.prettyNewLine());
        }

        if (anyUnchanged) {
            stringBuilder.append(spaces.render());

            int unchanged = total - numPrinted;
            stringBuilder.append("... ")
                .append(unchanged)
                .append(spaces.space()).append("additional field")
                .append(unchanged > 1 ? "s" : "")
                .append(spaces.space()).append("...");
            stringBuilder.append(spaces.prettyNewLine());
        }
    }

    void renderContainerChangeSuffix(@Nonnull final StringBuilder stringBuilder,
                                     @Nonnull final SpaceRenderer spaces) {
        switch (getDifferenceType()) {
            case REPLACEMENT:
                stringBuilder
                    .append(spaces.space()).append("-->").append(spaces.space())
                    .append(replacementValue);
                return;
            default:
                // Do nothing
        }
    }

    /**
     * Get the difference type associated with the {@link OperationType}.
     *
     * @param op The {@link OperationType} whose {@link DifferenceType} should be mapped.
     * @return The {@link DifferenceType} associated with the {@link OperationType}.
     */
    DifferenceType associatedDifferenceType(OperationType op) {
        switch (op) {
            case ADD:
                return DifferenceType.ADDITION;
            case REMOVE:
                return DifferenceType.REMOVAL;
            case REPLACE:
                return DifferenceType.REPLACEMENT;
            default:
                throw new IllegalArgumentException("Unknown operation type " + op);
        }
    }

    private void addToPath(@Nonnull final Deque<String> pathDeque) {
        if (fieldName != null) {
            pathDeque.addFirst(fieldName);
            parent.addToPath(pathDeque);
        }
    }

    protected void setFieldName(@Nonnull final String newFieldName) {
        Objects.requireNonNull(newFieldName);
        fieldName = newFieldName;
    }

    /**
     * A key-value object with children specified by name.
     */
    public static class JsonDiffObject extends JsonDiffField {
        private final HashMap<String, JsonDiffField> children = new HashMap<>();

        public JsonDiffObject() {
        }

        public JsonDiffObject(@Nonnull final String fieldName,
                             @Nonnull final JsonDiffField parentField) {
            super(fieldName, parentField);
        }

        @Override
        protected void addChild(@Nonnull final String fieldName,
                                @Nonnull final JsonNode associatedNode) {
            final JsonDiffField child = newField(associatedNode, fieldName, this);
            children.putIfAbsent(fieldName, child); // Don't overwrite an existing child if we have it because
                                                    // we associate state with the existing child.
            child.addChildren(associatedNode);
        }

        @Override
        public boolean factorDifference(@Nonnull final Deque<String> path,
                                        @Nonnull final RootJsonNode rootNode,
                                        @Nonnull final JsonDiffDescription diffDescription) {
            if (path.size() == 0) {
                return super.factorDifference(path, rootNode, diffDescription);
            }

            setDifferenceType(DifferenceType.CHILD_MODIFIED);
            final String nextOffset = path.removeFirst();

            JsonDiffField child = children.get(nextOffset);
            if (child == null) {
                // Create the child.
                final DifferenceType diffType = associatedDifferenceType(diffDescription.getOp());
                Preconditions.checkArgument(diffType == DifferenceType.ADDITION);
                final Deque<String> childPath = constructPath();

                final JsonNode childNode = applyAddition(rootNode, diffDescription, childPath);
                addChildren(childNode);
                child = children.get(nextOffset);
            }

            child.factorDifference(path, rootNode, diffDescription);

            return true;
        }

        @Override
        protected void renderDifference(@Nonnull final JsonNode node,
                                        @Nullable final String fieldName,
                                        @Nonnull final StringBuilder stringBuilder,
                                        @Nonnull final SpaceRenderer spaces,
                                        @Nonnull final Verbosity verbosity) throws JsonProcessingException {
            stringBuilder
                .append("{")
                .append(children.size() > 0 ? spaces.prettyNewLine() : spaces.space());

            renderChildrenToBuilder(node, stringBuilder, spaces, verbosity);
            stringBuilder
                .append(children.size() > 0 ? spaces.render() : "")
                .append("}");

            renderContainerChangeSuffix(stringBuilder, spaces);
        }

        @Override
        public void setDifferenceType(DifferenceType differenceType) {
            if (differenceType.ordinal() > this.differenceType.ordinal()) {
                this.differenceType = differenceType;
                if (differenceType == DifferenceType.ADDITION || differenceType == DifferenceType.REMOVAL) {
                    children.values().forEach(child -> child.setDifferenceType(differenceType));
                }
            }
        }

        private JsonNode applyAddition(@Nonnull final RootJsonNode rootNode,
                                            @Nonnull final JsonDiffDescription diffDescription,
                                            @Nonnull final Deque<String> childPath) {
            rootNode.applyDiff(diffDescription);
            return rootNode.getNodeAt(childPath);
        }

        protected void renderChildrenToBuilder(@Nonnull final JsonNode node,
                                               @Nonnull final StringBuilder stringBuilder,
                                               @Nonnull final SpaceRenderer spaces,
                                               final Verbosity verbosity) throws JsonProcessingException {
            int numPrinted = 0;
            final int numUnprintedChildren = children.entrySet().stream()
                    .mapToInt(entry -> shouldPrint(entry, verbosity) ? 0 : 1)
                    .sum();

            // Print in order to get a consistent order
            final List<String> sortedFieldNames = new ArrayList<>(children.keySet());
            Collections.sort(sortedFieldNames);

            for (String fieldName : sortedFieldNames) {
                final JsonDiffField child = children.get(fieldName);
                if (shouldPrint(fieldName, child, verbosity, numUnprintedChildren)) {
                    if (numPrinted > 0) {
                        stringBuilder.append(",").append(spaces.prettyNewLine());
                    }

                    child.renderToBuilder(fieldName, stringBuilder, node,
                        spaces.childRenderer(child.differenceType), verbosity);
                    numPrinted++;
                }
            }

            boolean anyPrinted = numPrinted > 0;
            if (verbosity == Verbosity.LOCAL_CONTEXT_VERBOSITY) {
                renderAdditional(numPrinted, anyPrinted, children.size(),
                    stringBuilder, spaces.childRenderer(getDifferenceType()));
            } else if (anyPrinted) {
                stringBuilder.append(spaces.prettyNewLine());
            }
        }

        private boolean shouldPrint(@Nonnull final Entry<String, JsonDiffField> childEntry,
                                    @Nonnull final Verbosity verbosity) {
            return shouldPrint(childEntry.getKey(), childEntry.getValue(), verbosity);
        }

        private boolean shouldPrint(@Nonnull final String fieldName,
                                    @Nonnull final JsonDiffField child,
                                    @Nonnull final Verbosity verbosity) {
            return verbosity == Verbosity.COMPLETE_VERBOSITY ||
                child.hasChanges() ||
                requiredFieldsToRetain.contains(fieldName) ||
                (verbosity == Verbosity.LOCAL_CONTEXT_VERBOSITY && contextFieldsToRetain.contains(fieldName));
        }

        private boolean shouldPrint(@Nonnull final String fieldName,
                                    @Nonnull final JsonDiffField child,
                                    @Nonnull final Verbosity verbosity,
                                    final int numUnprintedChildren) {
            return shouldPrint(fieldName, child, verbosity) ||
                (verbosity == Verbosity.LOCAL_CONTEXT_VERBOSITY && numUnprintedChildren < 2);
        }
    }

    /**
     * An array with children specified by index.
     */
    public static class JsonDiffArray extends JsonDiffField {
        private List<JsonArrayChild> children = new ArrayList<>();

        public JsonDiffArray() {
        }

        public JsonDiffArray(@Nonnull final String fieldName,
                             @Nonnull final JsonDiffField parentField) {
            super(fieldName, parentField);
        }

        @Override
        public void addChildren(@Nonnull final JsonNode associatedNode) {
            int childIndex = 0;
            for (JsonNode childNode : associatedNode) {
                addChild(Integer.toString(childIndex), childNode);
                childIndex++;
            }
        }

        @Override
        protected void addChild(@Nonnull final String fieldName,
                             @Nonnull final JsonNode associatedNode) {
            int index = Integer.parseInt(fieldName);
            final JsonDiffField child = newField(associatedNode, fieldName, this);

            if (index == children.size()) {
                children.add(new JsonArrayChild(child, index));
            } else if (index < children.size()) {
                children.add(index, new JsonArrayChild(child, index));
            } else {
                throw new IllegalArgumentException("Index " + index + " is out of bounds.");
            }

            child.addChildren(associatedNode);
        }

        @Override
        public boolean factorDifference(@Nonnull final Deque<String> path,
                                        @Nonnull final RootJsonNode rootNode,
                                        @Nonnull final JsonDiffDescription diffDescription) {
            if (path.size() == 0) {
                return super.factorDifference(path, rootNode, diffDescription);
            }

            setDifferenceType(DifferenceType.CHILD_MODIFIED);
            final String nextOffset = path.removeFirst();
            final int index = Integer.parseInt(nextOffset);

            DifferenceType diffType = associatedDifferenceType(diffDescription.getOp());
            if (path.isEmpty()) {
                if (diffType == DifferenceType.ADDITION || diffType == DifferenceType.REMOVAL) {
                    final JsonArrayChild child = applyAdditionOrRemoval(rootNode, diffDescription, index, diffType);
                    child.field.factorDifference(path, rootNode, diffDescription);
                    updateAfterAdditionOrRemoval(index, child, diffType);

                    return true;
                }
            }

            final JsonArrayChild child = getCurrentChildAt(index);
            child.field.factorDifference(path, rootNode, diffDescription);

            return true;
        }

        @Override
        protected void renderDifference(@Nonnull final JsonNode node,
                                        @Nullable final String fieldName,
                                        @Nonnull final StringBuilder stringBuilder,
                                        @Nonnull final SpaceRenderer spaces,
                                        @Nonnull final Verbosity verbosity) throws JsonProcessingException {
            stringBuilder.append("[").append(spaces.prettyNewLine());
            renderChildrenToBuilder(node, stringBuilder, spaces, verbosity);
            stringBuilder.append(spaces.render()).append("]");

            renderContainerChangeSuffix(stringBuilder, spaces);
        }

        @Nonnull
        protected JsonArrayChild getCurrentChildAt(int index) {
            return children.stream()
                .filter(child -> child.getCurArrayPosition() == index)
                .findFirst()
                .orElseThrow(() -> new IndexOutOfBoundsException("No current child at index: " + index));
        }

        @Override
        public void setDifferenceType(DifferenceType differenceType) {
            if (differenceType.ordinal() > this.differenceType.ordinal()) {
                this.differenceType = differenceType;
                if (differenceType == DifferenceType.ADDITION || differenceType == DifferenceType.REMOVAL) {
                    children.forEach(child -> child.field.setDifferenceType(differenceType));
                }
            }
        }

        protected void renderChildrenToBuilder(@Nonnull final JsonNode node,
                                               @Nonnull final StringBuilder stringBuilder,
                                               @Nonnull final SpaceRenderer spaces,
                                               @Nonnull final Verbosity verbosity) throws JsonProcessingException {
            int numChanged = 0;
            int numUnmodifiedChildren = children.stream()
                .mapToInt(child -> child.field.differenceType == DifferenceType.UNMODIFIED ? 1 : 0)
                .sum();
            final boolean renderAll = (verbosity == Verbosity.COMPLETE_VERBOSITY) ||
                (verbosity == Verbosity.LOCAL_CONTEXT_VERBOSITY && (
                    numUnmodifiedChildren <= 3 ||
                    getDifferenceType() == DifferenceType.ADDITION ||
                    getDifferenceType() == DifferenceType.REMOVAL
                ));

            for (JsonArrayChild child : children) {
                if (child.field.hasChanges() || renderAll) {
                    if (numChanged > 0) {
                        stringBuilder.append(",").append(spaces.prettyNewLine());
                    }

                    child.field.renderToBuilder(stringBuilder, node,
                        spaces.childRenderer(child.field.differenceType), verbosity);
                    numChanged++;
                }
            }

            boolean anyChanged = numChanged > 0;
            if (verbosity == Verbosity.LOCAL_CONTEXT_VERBOSITY) {
                renderAdditional(numChanged, anyChanged, children.size(),
                    stringBuilder, spaces.childRenderer(getDifferenceType()));
            } else if (anyChanged) {
                stringBuilder.append(spaces.prettyNewLine());
            }
        }

        public JsonArrayChild applyAdditionOrRemoval(@Nonnull final RootJsonNode rootNode,
                                                     @Nonnull final JsonDiffDescription diffDescription,
                                                     final int index,
                                                     @Nonnull final DifferenceType differenceType) {
            rootNode.applyDiff(diffDescription);

            JsonArrayChild child;
            if (differenceType == DifferenceType.ADDITION) {
                final JsonNode associatedNode = rootNode.getNodeAt(diffDescription.createPathDeque());

                child = new JsonArrayChild(JsonDiffField.newField(associatedNode), index);
                child.field.addChildren(associatedNode);
                children.add(child);
            } else {
                child = getCurrentChildAt(index);
            }

            return child;
        }

        public void updateAfterAdditionOrRemoval(final int index,
                                                 @Nonnull final JsonArrayChild affectedChild,
                                                 @Nonnull final DifferenceType differenceType) {
            int indexOffset = differenceType == DifferenceType.ADDITION ? 1 : -1;

            children.stream()
                .filter(child -> child.field.getDifferenceType() != DifferenceType.REMOVAL)
                .filter(child -> child.getCurArrayPosition() >= index)
                .filter(child -> child != affectedChild)
                .forEach(child -> child.shiftIndexBy(indexOffset));

            // Sort the children so that they remain in the order that we will want to print them.
            Collections.sort(children);
        }
    }

    public static class RootJsonNode {
        @Nonnull
        private JsonNode node;

        public RootJsonNode(@Nonnull final JsonNode node) {
            this.node = Objects.requireNonNull(node);
        }

        public void applyDiff(@Nonnull final JsonDiffDescription diffDescription) {
            final ArrayNode change = new JsonNodeFactory(false).arrayNode();
            change.add(diffDescription.getAssociatedJsonDiff());

            node = JsonPatch.apply(change, node);
        }

        public JsonNode getNodeAt(@Nonnull final Deque<String> path) {
            JsonNode cur = node;
            while (!path.isEmpty()) {
                if (cur.isArray()) {
                    cur = cur.get(Integer.parseInt(path.removeFirst()));
                } else {
                    cur = cur.get(path.removeFirst());
                }
            }

            return cur;
        }
    }

    public static class JsonArrayChild implements Comparable<JsonArrayChild> {
        public final JsonDiffField field;
        public final JsonArrayIndexTracker indices;

        public JsonArrayChild(@Nonnull final JsonDiffField field, final int initialIndex) {
            this.field = Objects.requireNonNull(field);
            this.indices = new JsonArrayIndexTracker(initialIndex);
        }

        public void shiftIndexBy(final int offsetForNextIndex) {
            int priorIndex = indices.latestPosition();
            if (priorIndex + offsetForNextIndex < 0) {
                throw new ArrayIndexOutOfBoundsException("Unable to shift index " + priorIndex
                    + " by " + offsetForNextIndex + " because the new index is invalid");
            }

            indices.historicalPositions.add(0, priorIndex + offsetForNextIndex);
            field.setFieldName(Integer.toString(priorIndex + offsetForNextIndex));
        }

        @Override
        public int compareTo(final JsonArrayChild child) {
            return indices.compareTo(child.indices);
        }

        // Return latest position. If child was removed, return -1
        public int getCurArrayPosition() {
            if (field.getDifferenceType() == DifferenceType.REMOVAL) {
                return -1;
            }

            return indices.latestPosition();
        }
    }

    public static class JsonArrayIndexTracker implements Comparable<JsonArrayIndexTracker> {
        // The current position is at the front of the list.
        private final List<Integer> historicalPositions = new ArrayList<>();

        public JsonArrayIndexTracker(int originalPosition) {
            historicalPositions.add(originalPosition);
        }

        @Override
        public int compareTo(@Nonnull final JsonArrayIndexTracker other) {
            int stop = Math.min(historicalPositions.size(), other.historicalPositions.size());
            for (int i = 0; i < stop; i++) {
                int curComparison = Integer.compare(historicalPositions.get(i), other.historicalPositions.get(i));
                if (curComparison != 0) {
                    return curComparison;
                }
            }

            return Integer.compare(historicalPositions.size(), other.historicalPositions.size());
        }

        public int latestPosition() {
            return historicalPositions.get(0);
        }
    }
}
