package com.vmturbo.search;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

/**
 * Utility class to help construct pagination related fields.
 */
public class SearchPaginationUtil {

    /**
     * Prefix to designate cursor types.
     */
    public enum CursorType {
        /**
         * NEXT.
         */
        NEXT,
        /**
         * PREVIOUS.
         */
        PREVIOUS
    }

    /**
     * Hidden Constructor.
     */
    private SearchPaginationUtil(){
    }

    //Delimiting char for write/read of cursors
    private static final CharSequence DELIMITER = ":";

    /**
     * Constructs next cursor representation of passed values.
     *
     * @param cursorValues values making up the cursor
     * @return cursor
     */
    @Nonnull
    public static String constructNextCursor(@Nonnull List<String> cursorValues) {
        cursorValues.add(0, CursorType.NEXT.name());
        return String.join(DELIMITER, cursorValues);
    }

    /**
     * Constructs previous cursor representation of passed values.
     *
     * @param cursorValues values making up the cursor
     * @return cursor
     */
    @Nonnull
    public static String constructPreviousCursor(@Nonnull List<String> cursorValues) {
        cursorValues.add(0, CursorType.PREVIOUS.name());
        return String.join(DELIMITER, cursorValues);
    }

    /**
     * Parse cursor values to List.
     *
     * @param cursor to be parsed
     * @return collection of cursor values
     */
    @Nonnull
    private static List<String> parseCursor(@Nonnull String cursor) {
        return Arrays.asList(cursor.split(DELIMITER.toString()));
    }

    /**
     * Parse cursor values to List.
     *
     * @param cursor to be parsed
     * @return collection of cursor values
     */
    @Nonnull
    public static List<String> getCursorWithoutPrefix(@Nonnull String cursor) {
        List<String> cursorValues = parseCursor(cursor);
        return cursorValues.subList(1, cursorValues.size());
    }

    /**
     * Identifies if previous cursor configured.
     * @param cursor Cursor to read
     * @return true if previous cursor configured
     */
    @Nonnull
    public static boolean isPreviousCursor(@Nonnull String cursor) {
        return parseCursor(cursor).get(0).equals(CursorType.PREVIOUS.name());
    }

    /**
     * Identifies if next cursor is configured.
     * @param cursor Cursor to read
     * @return true if next cursor configured
     */
    @Nonnull
    public static boolean isNextCursor(@Nonnull String cursor) {
        return parseCursor(cursor).get(0).equals(CursorType.NEXT.name());
    }
}
