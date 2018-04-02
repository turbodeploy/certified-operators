package com.vmturbo.stitching.journal;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

public class JsonDiffDescription {
    // Lowercase for easy Jackson deserialization.
    public enum OperationType {
        @JsonProperty("add")
        ADD,

        @JsonProperty("remove")
        REMOVE,

        @JsonProperty("replace")
        REPLACE,

        @JsonProperty("move")
        MOVE,

        @JsonProperty("copy")
        COPY
    }

    private OperationType op;
    private String path;
    private String from;
    private JsonNode value;
    private JsonNode associatedJsonDiff;

    public OperationType getOp() {
        return op;
    }

    public void setOp(OperationType op) {
        this.op = op;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public JsonNode getValue() {
        return value;
    }

    public void setValue(JsonNode value) {
        this.value = value;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public JsonNode getAssociatedJsonDiff() {
        return associatedJsonDiff;
    }

    public void setAssociatedJsonDiff(@Nonnull final JsonNode associatedJsonDiff) {
        this.associatedJsonDiff = Objects.requireNonNull(associatedJsonDiff);
    }

    @Override
    public String toString() {
        String descripton = op + " " + path;
        if (value != null) {
            descripton += " " + value;
        }
        if (from != null) {
            descripton += " " + from;
        }
        return descripton;
    }

    @Nonnull
    public Deque<String> createPathDeque() {
        // The path of the change starts from the from field if present. If not, starts from the path field.
        final String toParse = (from != null) ? from : path;

        if (toParse == null || toParse.isEmpty()) {
            return new ArrayDeque<>();
        } else {
            if (!toParse.startsWith("/")) {
                throw new IllegalArgumentException("JsonDiffDescription paths must start with '/'");
            }

            final String[] pathArray = toParse.split("/");
            final Deque<String> pathDeque = new ArrayDeque<>(pathArray.length);
            Collections.addAll(pathDeque, pathArray);

            // Remove the leading element because it will be empty due to leading '/' if the path is non-empty
            if (!pathDeque.isEmpty()) {
                pathDeque.removeFirst();
            }

            return pathDeque;
        }
    }
}
