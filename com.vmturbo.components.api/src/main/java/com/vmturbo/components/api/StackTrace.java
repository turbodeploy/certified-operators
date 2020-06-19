package com.vmturbo.components.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Utility class to help print stack trace and caller-related information for logging purposes.
 */
public class StackTrace {
    private static final int JAVA_SOURCE_FILE_SUFFIX_LENGTH = ".java".length();

    /**
     * This is a utility class. No instantiation allowed.
     */
    private StackTrace() {
    }

    /**
     * Get the file name and the line number of the first stack trace element of
     * the top of the stack not in the caller's method. Ignores certain uninteresting stack trace
     * elements (e.g. Optional.ifPresent() internals).
     *
     * @return The file and line number of the caller in the call stack. Empty string if it can't
     *         figure out the stack trace.
     */
    @Nonnull
    public static String getCaller() {
        final StackTraceElement callerElement = getCallerElement(0);
        return callerElement == null ? "" : callerElement.getFileName() + ":" + callerElement.getLineNumber();
    }

    /**
     * Get the file name and the line number of the first stack trace element of
     * the top of the stack not in the caller's class itself. Ignores certain uninteresting stack
     * trace elements (e.g. Optional.ifPresent() internals).
     *
     * @return The file and line number of the caller in the call stack. Empty string if it can't
     *         figure out the stack trace.
     */
    @Nonnull
    public static String getCallerOutsideClass() {
        final StackTraceElement[] stackTrace = new Throwable().getStackTrace();
        if (stackTrace != null) {
            String outerElementClassName = null;
            for (StackTraceElement element : stackTrace) {
                if (StackTrace.isInterestingCallSite(element)) {
                    if (outerElementClassName == null) {
                        outerElementClassName = element.getClassName();
                    } else if (!outerElementClassName.equals(element.getClassName())) {
                        return element.getFileName() + ":" + element.getLineNumber();
                    }
                }
            }
        }
        return "";
    }

    @Nullable
    private static StackTraceElement getCallerElement(final int depth) {
        final StackTraceElement[] stackTrace = new Throwable().getStackTrace();
        if (stackTrace != null) {
            int outerElementCnt = 0;
            for (StackTraceElement element : stackTrace) {
                if (StackTrace.isInterestingCallSite(element)) {
                    outerElementCnt++;
                    if (outerElementCnt > depth + 1) {
                        return element;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Check whether the stack trace element is interesting enough to track. Stack frames from within
     * the stack source library or within the Java library code (ie. java.util.stream, java.util.Collection, etc.)
     * are not considered interesting because they don't actually help someone who is debugging figure out
     * where in the source code to look for the related calculation.
     *
     * <p>For example, if we do "Optional.ifPresent() -> ... StackTrace.getCaller())" we want to
     * return the call site of Optional.ifPresent(), not the call site inside the Optional class
     * that actually calls the lambda.
     *
     * @param element The stack trace element to check
     * @return Whether or not this file is an interesting call site to track.
     */
    private static boolean isInterestingCallSite(@Nonnull final StackTraceElement element) {
        final String fileName = element.getFileName();
        if (fileName == null) {
            return false;
        }
        final int len = fileName.length();
        if (len < JAVA_SOURCE_FILE_SUFFIX_LENGTH) {
            return false;
        }

        final String withoutJavaSuffix = fileName.substring(0, len - JAVA_SOURCE_FILE_SUFFIX_LENGTH);

        return (!withoutJavaSuffix.startsWith(StackTrace.class.getSimpleName()) || withoutJavaSuffix.endsWith("Test")) &&
            !element.getClassName().startsWith("java");
    }
}
