package com.vmturbo.action.orchestrator.template;

import javax.annotation.Nonnull;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.velocity.tools.generic.EscapeTool;

/**
 * EscapeToolWithJson extends EscapeTool to  offer escaping methods for json.
 */
public class EscapeToolWithJson extends EscapeTool {

    /**
     * Escapes the characters for a JSON string.
     * @param string the string to escape values.
     * @return string with escaped values
     */
    public String json(@Nonnull String string) {
        return StringEscapeUtils.escapeJson(string);
    }
}
