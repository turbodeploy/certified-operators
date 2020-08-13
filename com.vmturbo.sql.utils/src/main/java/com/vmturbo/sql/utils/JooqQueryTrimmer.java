package com.vmturbo.sql.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.tuple.Pair;
import org.jooq.Query;
import org.jooq.exception.DataAccessException;

/**
 * Methods to trim jOOQ query strings that can appear in very unreasonable from in our logs.
 */
public class JooqQueryTrimmer {

    /** disallow instantiation. */
    private JooqQueryTrimmer() {
    }

    /**
     * This method deals with a very unfortunate feature of some jOOQ query renderings in exception
     * logs, where very long and nearly information-free placeholder lists often appear across
     * multiple lines.
     *
     * <p>In the case of a multi-valued insert statement with inline values, this ends up including
     * many repetitions (one per record), as in "(?, ?, ?, ?), (?, ?, ?), ...".</p>
     *
     * <p>The methods of this class can will elide placeholders in two ways to reduce the noise:</p>
     * <ul>
     *     <li>Within a single placehoder list, all but the first two placeholdes will be elided,
     *     with an indication after the elision of the number of elided placeholders. E.g.
     *     "(?, ?, ?, ?, ?)" becomes "(?, ?, ...[+3])"</li>
     *     <li>When there are mutliple placeholder lists in sequence, all but the first are elided,
     *     with number additional lists provided after the elision. For example, "(?, ?), (?, ?)"
     *     becomes "(?, ?), ...[+1]"</li>
     * </ul>
     *
     * @param sql SQL string to be trimmed
     * @return trimmed SQL
     */
    public static String trimJooqQueryString(@Nonnull String sql) {
        // first make this a single-line string
        sql = sql.replaceAll("[\\r\\n]+", " ");
        // now find all the embeded placeholder lists
        List<PlaceholderList> matches = getPlaceholderLists(sql);
        if (matches.isEmpty()) {
            // no placeholder lists
            return sql;
        }
        // now piece the final elided string together, starting with what precedes first match
        final Iterator<PlaceholderList> matchIter = matches.iterator();
        PlaceholderList prior = null;
        int runLength = 0;
        StringBuilder trimmedBuilder = new StringBuilder();
        // start with text prior to first match
        trimmedBuilder.append(sql, 0, matches.get(0).getStart());
        while (matchIter.hasNext()) {
            final PlaceholderList match = matchIter.next();
            if (prior != null) {
                // see whether we're part of a sequence of consecutive matches
                String inter = sql.substring(prior.getEnd(), match.getStart());
                if (inter.matches("\\s*,\\s*")) {
                    // yes, just add this one to the count; it will be elided
                    runLength += 1;
                } else {
                    // no... add elision of 2nd and subsequent matches in current run, if needed
                    trimmedBuilder.append(runLength > 1
                            ? String.format(", ...[+%s]", runLength - 1)
                            : "");
                    // and any text between that run and this match
                    trimmedBuilder.append(inter);
                    // and finally, the elision of this match, which is the first in a new run
                    trimmedBuilder.append(match.elide());
                    runLength = 1;
                }
            } else {
                // first match... add its elision and start a run
                trimmedBuilder.append(match.elide());
                runLength = 1;
            }
            prior = match;
        }
        // finish up with elision of final run, if needed
        trimmedBuilder.append(runLength > 1 ? String.format(", ...[+%s]", runLength - 1) : "");
        // and any text following that run
        trimmedBuilder.append(sql.substring(prior.getEnd()));
        return trimmedBuilder.toString();
    }

    /**
     * Find all the placeholder lists in the given query string.
     *
     * @param sql query string
     * @return placeholder lists
     */
    @Nonnull
    private static List<PlaceholderList> getPlaceholderLists(final @Nonnull String sql) {
        List<PlaceholderList> matches = new ArrayList<>();
        for (PlaceholderList match = PlaceholderList.find(sql, 0);
             match != null;
             match = PlaceholderList.find(sql, match.getEnd())
        ) {
            matches.add(match);
        }
        return matches;
    }

    /**
     * Class to locate and represent an appearance of a placeholder list within a query string.
     *
     * <p>A placeholder list is any comma-separate list of placeholders (question-mark characters),
     * enclosed in a pair of parentheses, and with arbitrary interior whitespace.</p>
     */
    private static class PlaceholderList {
        // position in sql string where this placeholder list starts
        int start;
        // position in sql string immediately following placeholder list
        int end;
        // number of placeholders in the list
        int phCount;

        public int getStart() {
            return start;
        }

        public int getEnd() {
            return end;
        }

        /**
         * Return a canonical representation of this placeholder list, with any placeholders beyond
         * the second replaced with an elision ("...") followed by the number of elided
         * placeholders.
         *
         * @return the elided list
         */
        public String elide() {
            switch (phCount) {
                case 1:
                    return "(?)";
                case 2:
                    return "(?, ?)";
                default:
                    return String.format("(?, ?, ...[+%d])", phCount - 2);
            }
        }

        private PlaceholderList(int start, int end, int phCount) {
            this.start = start;
            this.end = end;
            this.phCount = phCount;
        }

        /**
         * Find placeholder list in the given string, starting at or after the given position.
         *
         * @param s    string in which to find placeholder list
         * @param from starting position for search
         * @return next placeholder list, or null if none found
         */
        public static PlaceholderList find(String s, int from) {
            Integer start;
            Pair<Integer, Integer> end;
            while (from < s.length()) {
                // find an open-paren to start a list
                start = findStart(s, from);
                // skip past placeholders to matching close paren
                end = start != null ? findEnd(s, start + 1) : null;
                if (end != null) {
                    // package up the placeholder list if we found a complete list
                    return new PlaceholderList(start, end.getLeft(), end.getRight());
                } else {
                    // otherwise start again, but skip past an open paren if we found one
                    // note that if we didn't find a start, then we're done, so move to end of
                    // string so loop will exit
                    from = start != null ? start + 1 : s.length();
                }
            }
            return null;
        }

        /**
         * Find what may be the beginning of a placeholder list.
         *
         * @param s     string to search
         * @param start postition at which to begin earch
         * @return position of potential start, if found
         */
        private static Integer findStart(String s, int start) {
            while (start < s.length()) {
                // we're just looking for an open paren
                if (s.charAt(start) == '(') {
                    return start;
                } else {
                    start += 1;
                }
            }
            return null;
        }

        /**
         * Scan over text following an open-paren to see whether it looks like a placeholder list,
         * and if so return its end position & placeholder count.
         *
         * @param s   string to scan
         * @param pos position to start scan (immediately following open-parn)
         * @return position following placeholder list, and count of placeholders, or null if scan
         * failed
         */
        private static Pair<Integer, Integer> findEnd(String s, int pos) {
            // we can only accept end of list immediately after a placholder, which prevents
            // matching an empty placeholder list
            boolean canEnd = false;
            int phCount = 0;
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (Character.isWhitespace(c)) {
                    // ignore all whitespace
                    pos += 1;
                } else if (canEnd && c == ')') {
                    // we found the end of a non-empty placeholder list
                    return Pair.of(pos + 1, phCount);
                } else if (canEnd && c == ',') {
                    // if we see a comma after a placeholder we can't end without at least
                    // one more placeholder
                    canEnd = false;
                    pos += 1;
                } else if (!canEnd && c == '?') {
                    // a placeholder means we can now end the list, after counting this one
                    canEnd = true;
                    phCount += 1;
                    pos += 1;
                } else {
                    // anything that doesn't fit the above means that this scan failed
                    break;
                }
            }
            // here if we either saw something that wasn't OK, or we reached the end of the
            // string without a complete placeholder list
            return null;
        }
    }

    /**
     * Renders a jOOQ query's SQL with trimming.
     *
     * @param query query whose trimmed SQL is needed
     * @return the trimmed SQL string
     */
    public static String trimJooqQuery(@Nonnull Query query) {
        return trimJooqQueryString(query.getSQL());
    }

    /**
     * Renders a query string embedded in the message of a jOOQ {@link DataAccessException}, with
     * trimming.
     *
     * @param e exception whose message should be trimmed
     * @return the trimmed exception message
     */
    @Nonnull
    public static String trimJooqErrorMessage(@Nonnull Exception e) {
        if (e instanceof DataAccessException) {
            // this is a JOOQ exception, possibly with the query SQL inlined into its message
            return trimJooqQueryString(e.toString());
        } else {
            return e.toString();
        }
    }

}
