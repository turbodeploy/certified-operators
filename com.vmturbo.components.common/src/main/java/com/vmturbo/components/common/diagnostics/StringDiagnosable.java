package com.vmturbo.components.common.diagnostics;

import javax.annotation.Nonnull;

/**
 * The {@link StringDiagnosable} is an object that should be included into the diagnostics
 * provided
 * by the component. Any object can implement this interface, and it's the responsibility of the
 * component author to actually inject all objects implementing this interface into the
 * object responsible for assembling the diags.
 *
 * <p>TODO (roman, Sept 7 2017): Right now most diagnostic collection happens outside of the objects
 * that own the data, so this interface isn't properly used. We should have the objects that
 * provide diags implement this interface, but this will require some refactoring.
 *
 * <p>TODO (roman, Sept 7 2017): The interface dealing with lists of strings is clumsy. It should
 * just be streams, and we should have a framework where a common diagnostics handler can
 * iterate through {@link StringDiagnosable}s and manage the streams given to each.
 */
public interface StringDiagnosable extends Diagnosable {
    /**
     * Save the diags as a sequence of strings sent to {@link DiagnosticsAppender}.
     *
     * @param appender an appender to put diagnostics to. String by string.
     * @throws DiagnosticsException When an exception occurs during diagnostics collection.
     */
    void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException;
}
