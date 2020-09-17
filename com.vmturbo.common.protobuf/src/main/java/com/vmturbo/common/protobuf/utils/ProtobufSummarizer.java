package com.vmturbo.common.protobuf.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;

/**
 * Class to render protobuf messages in a manner that's customized for logging.
 *
 * <p>By default, this renders the message as a compact, one-line JSON representation. However,
 * the summarizer can be customized for specific message types, in order to customize rendering of
 * specific message fields and/or exclude specific fields.</p>
 *
 * <p>A built-in field override is implemented by {@link ProtobufSummarizer#limitArray(List, int)},
 * which limits an array field value so that only a limited number of elements are rendered, and
 * remaining entities, if any, are rendered as an elision ("...") followed by a count of omitted
 * values, as in "...+3" if 3 values are omitted.</p>
 *
 * @param <T> Type of protobuf, should extend {@link Message} or {@link Message.Builder} or, for
 *            maximum applicability, {@link MessageOrBuilder}.
 */
public class ProtobufSummarizer<T extends MessageOrBuilder> {
    private static final Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
    private static final Gson gson = new Gson();

    private final Set<String> excludedFields;
    private final boolean excludeDefaultValues;
    private final Map<String, FieldOverride> overrides;
    private final Class<? extends Message> msgType;

    private ProtobufSummarizer(@Nonnull Class<T> type,
            @Nonnull Class<? extends Message> msgType,
            @Nonnull Set<String> excludedFields,
            boolean excludeDefaultValues,
            @Nonnull Map<String, FieldOverride> overrides) {
        this.msgType = msgType;
        this.excludedFields = excludedFields;
        this.excludeDefaultValues = excludeDefaultValues;
        this.overrides = overrides;
    }

    /**
     * Create a builder that can be used to create a new summarizer.
     *
     * @param type    class corresponding to protobuf {@link MessageOrBuilder} type
     * @param msgType class corresponding to protobuf {@link Message} type
     * @param <T>     protobuf {@link MessageOrBuilder} type
     * @return the new builder
     */
    public static <T extends MessageOrBuilder> Builder<T> of(
            Class<T> type, Class<? extends Message> msgType) {
        return new Builder<T>(type, msgType);
    }

    /**
     * Render a protobuf message with the given inclusion filter and given rendering override
     * function.
     *
     * <p>The field filter, if not null, is passed field names and decides whether or not the named
     * field should be included in the rendered message. The override function is also passed a
     * field name and can either return a rendered field value or null, to indicate that standard
     * rendering should be used.</p>
     *
     * @param msg protobuf message value
     * @return rendered result
     */
    public String summarize(T msg) {
        // loop over the message fields
        final String objectData = msg.getAllFields().keySet().stream()
                .filter(field -> shouldIncludeField(msg, field))
                .map(field -> {
                    // get initial syntax for the field name as a JSON property
                    final String fieldName = field.getName();
                    final String prefix = gson.toJson(fieldName) + ":";
                    // give override function first crack, if there is one
                    final Object fieldValue = msg.getField(field);
                    String value = overrides.getOrDefault(fieldName, (n, v) -> toJson(v))
                            .apply(fieldName, fieldValue);
                    // render JSON property name+value syntax
                    return prefix + value;
                })
                .collect(Collectors.joining(","));
        // and collect it all into a proper JSON object
        return "{" + objectData + "}";
    }

    /**
     * Render an array as something that looks like a JSON array value, but in which elements in the
     * tail may be replaced with an elision specifying the number omitted elements, as in "...+10"
     * in the case that the final 10 elements are omitted
     *
     * <p>Rendered elements are rendered using {@link ProtobufSummarizers} if they are of a
     * protobuf message type; otherwise they are rendered to JSON using Gson's built-in type
     * adapters. Errors in Gson rendering result in the string "(unrenderable)" replacing the
     * failing value in the overall rendered result.</p>
     *
     * @param values        values to be rendered
     * @param renderedCount max number of values to render; additional elements, if any, are
     *                      replaced with an elision (...)
     * @return rendered result
     */
    @Nonnull
    static String limitArray(List<?> values, int renderedCount) {
        final int omittedCount = Math.max(0, values.size() - renderedCount);
        // render intiial values, with intervening commas, if any
        final String rendered = values.subList(0, values.size() - omittedCount).stream()
                .map(ProtobufSummarizer::toJson)
                .collect(Collectors.joining(","));
        // render the elision, if there are any elided values
        final String elision = omittedCount > 0 ? "...+" + (values.size() - renderedCount) : "";
        // if we actually have both an unelided part and an elided part, separate them with a comma
        final String comma = renderedCount > 0 && omittedCount > 0 ? "," : "";
        // and put it all together
        return "[" + rendered + comma + elision + "]";
    }

    /**
     * Render a value as compact, single-line JSON.
     *
     * <p>If the value is a protobuf messages value, we use the built-in protobuf json renderer.
     * Else we use a Gson instance with standard type adapters.</p>
     *
     * @param obj value to be rendered
     * @return rendered string
     */
    static String toJson(Object obj) {
        if (obj instanceof MessageOrBuilder) {
            try {
                return printer.print((MessageOrBuilder)obj);
            } catch (InvalidProtocolBufferException e) {
                return "\"(unrenderable)\"";
            }
        } else if (obj instanceof List) {
            return "[" + ((List<?>)obj).stream()
                    .map(ProtobufSummarizer::toJson)
                    .collect(Collectors.joining(","))
                    + "]";
        } else {
            return gson.toJson(obj);
        }
    }

    /**
     * Decide whether a field should be included in the rendering.
     *
     * @param msg   message being rendered
     * @param field field descriptor of field being tested
     * @return true if the field should be rendered
     */
    private boolean shouldIncludeField(T msg, FieldDescriptor field) {
        if (excludedFields != null && excludedFields.contains(field.getName())) {
            // excluded by field filter
            return false;
        } else if (field.isRepeated()) {
            // empty repeated fields are excluded by default
            return msg.getRepeatedFieldCount(field) > 0;
        } else if (!msg.hasField(field)) {
            // not-present fields are excluded
            return false;
        } else if (excludeDefaultValues) {
            // exclude field whose values is the field default, if so configured
            return !isDefault(msg, field);
        } else {
            return true;
        }
    }

    private boolean isDefault(T msg, FieldDescriptor field) {
        return field.hasDefaultValue() && msg.getField(field).equals(field.getDefaultValue());
    }

    public Class<? extends Message> getType() {
        return msgType;
    }

    ProtobufSummarizer<T> register() {
        return ProtobufSummarizers.register(this);
    }

    /**
     * Builder class for {@link ProtobufSummarizer}.
     *
     * @param <T> protobuf type of summarizer to be built
     */
    public static class Builder<T extends MessageOrBuilder> {

        private final Set<String> excludedFields = new HashSet<>();
        private final Class<? extends Message> msgType;
        private final Class<T> type;
        private boolean excludeDefaultValues;
        private final Map<String, FieldOverride> overrides = new HashMap<>();

        private Builder(Class<T> type, Class<? extends Message> msgType) {
            this.type = type;
            this.msgType = msgType;
        }

        /**
         * Specify names of fields that should be excluded from the rendering.
         *
         * <p>This method may be called more than once, accumulating exclusions.</p>
         *
         * @param fieldNames field names to be excluded
         * @return this builder
         */
        public Builder<T> excludingFields(String... fieldNames) {
            Collections.addAll(excludedFields, fieldNames);
            return this;
        }

        /**
         * Specify that fields with default values should not be rendered.
         *
         * @return this builder
         */
        public Builder<T> excludingDefaultValues() {
            this.excludeDefaultValues = true;
            return this;
        }

        /**
         * Specify an override function for the named field.
         *
         * @param fieldName field name
         * @param override  override function to be used to render this field's value
         * @return this builder
         */
        public Builder<T> overriding(String fieldName, FieldOverride override) {
            overrides.put(fieldName, override);
            return this;
        }

        /**
         * Specify that a given repeated field's value should be limited to a maximum number of
         * rendered values, followed by an elision ("...+n)".
         *
         * @param fieldName name of field to be limited
         * @param n         max number of elements to render
         * @return this builder
         */
        public Builder<T> limitingArray(String fieldName, int n) {
            return overriding(fieldName, (name, v) -> limitArray((List<?>)v, n));
        }

        /**
         * Build this summarizer.
         *
         * @return the summarizer
         */
        public ProtobufSummarizer<T> build() {
            return new ProtobufSummarizer<>(type, msgType,
                    ImmutableSet.copyOf(excludedFields),
                    excludeDefaultValues,
                    ImmutableMap.copyOf(overrides));
        }
    }

    /**
     * A method to compute a rendered string for a given field value, as in (name, value) -> string.
     *
     * <p>The field name is provided in case it's useful when the same field override can be used
     * for multiple fields.</p>
     */
    @FunctionalInterface
    interface FieldOverride extends BiFunction<String, Object, String> {
    }
}
