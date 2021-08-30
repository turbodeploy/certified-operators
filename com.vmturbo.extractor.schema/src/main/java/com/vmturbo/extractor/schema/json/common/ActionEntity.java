package com.vmturbo.extractor.schema.json.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import com.vmturbo.extractor.schema.json.export.Entity;

/**
 * The entity used in action. It's a subclass of {@link Entity}, but only some basic fields (like
 * (oid, name, type, attrs.targets) are populated.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder({ "oid", "name", "type" })
@JsonIgnoreProperties({ "environment", "state", "attrs", "metric", "related", "accountExpenses", "cost" })
public class ActionEntity extends Entity {
}