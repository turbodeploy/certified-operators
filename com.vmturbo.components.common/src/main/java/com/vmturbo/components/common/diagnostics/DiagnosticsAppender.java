package com.vmturbo.components.common.diagnostics;

import javax.annotation.Nonnull;

/**
 * An object which is used to append diagnostics data represented by strings. It accepts as many
 * strings as required. Implementations are expected not to store all the strings in memory but
 * flush it to some storage/network transport instead.
 */
public interface DiagnosticsAppender {
    /**
     * Adds the next string to diagnostics. Every string could be later parsed as a separate string.
     * In order not to break parsing, do not put any newline symbols in the string.
     *
     * @param string string to append
     * @throws DiagnosticsException if underlying implementation failed to send the string
     *         further
     */
    void appendString(@Nonnull String string) throws DiagnosticsException;
}
