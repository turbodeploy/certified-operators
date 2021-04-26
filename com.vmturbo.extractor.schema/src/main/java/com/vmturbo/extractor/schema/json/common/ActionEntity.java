package com.vmturbo.extractor.schema.json.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import com.vmturbo.extractor.schema.json.export.Entity;

/**
 * The entity used in action. It's a subclass of {@link Entity}, but only some basic fields (like
 * (oid, name, type, attrs.targets) are populated.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class ActionEntity extends Entity {
}