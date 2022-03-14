package com.vmturbo.sql.utils;

import java.util.HashMap;

import javax.annotation.Nonnull;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.SQLDialect;

import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.pattern.matching.MariaDBPatternMatching;
import com.vmturbo.sql.utils.pattern.matching.PatternMatchingAdapter;
import com.vmturbo.sql.utils.pattern.matching.PostgresPatternMatching;

/**
 * Support multiple databases by resolving any discrepancies between them.
 */
public class MultiDB implements PatternMatchingAdapter {

    // Adapter to support multiple databases with pattern matching behavior.
    private final PatternMatchingAdapter patternMatchingAdapter;

    /**
     * Hash map containing the MultiDB instances corresponding to each supported SQL dialect.
     *
     * <p>On runtime, only one dialect will be used by each component, so this map will contain only
     * one element. However, we use multiple dialects per component when running the unit tests.
     * For this reason, the "multiton" pattern is preferred as a clean way to support this
     * one component using multiple databases case.</p>
     */
    private static final HashMap<SQLDialect, MultiDB> multiDBInstances = new HashMap<>();

    /**
     * Get a MultiDB support object based on the SQL dialect of the database.
     *
     * @param dialect The SQL dialect of the database.
     * @return Instance of MultiDB object that corresponds to the SQL {@code dialect}.
     * @throws UnsupportedDialectException In case the dialect of the SQL {@code dialect} is
     *         not supported.
     */
    @Nonnull
    public static synchronized MultiDB of(@Nonnull SQLDialect dialect) throws UnsupportedDialectException {
        // If a MultiDB object for this dialect does not exist, we create it and put it in the map.
        if (!multiDBInstances.containsKey(dialect)) {
            multiDBInstances.put(dialect, new MultiDB(dialect));
        }
        return multiDBInstances.get(dialect);
    }

    /**
     * Create MultiDB support object based on the SQL dialect.
     *
     * @param dialect The SQL dialect of the database.
     * @throws UnsupportedDialectException In case the dialect of the {@code dslContext} is not supported.
     */
    private MultiDB(@Nonnull SQLDialect dialect) throws UnsupportedDialectException {
        patternMatchingAdapter = getPatternMatchingAdapter(dialect);
    }

    /**
     * Get pattern matching adapter depending on the database dialect.
     *
     * @param dialect The SQL dialect of the database.
     * @return A pattern matching adapter depending on the database dialect.
     * @throws UnsupportedDialectException In case the dialect of the {@code dslContext} is not supported.
     */
    @Nonnull
    private static PatternMatchingAdapter getPatternMatchingAdapter(@Nonnull SQLDialect dialect)
            throws UnsupportedDialectException {
        switch (dialect) {
            case MARIADB:
            case MYSQL:
                return new MariaDBPatternMatching();
            case POSTGRES:
                return new PostgresPatternMatching();
            default:
                throw new UnsupportedDialectException(dialect);
        }
    }

    @Nonnull
    @Override
    public Condition matchRegex(@Nonnull Field<String> fieldToMatch, @Nonnull String regex,
            @Nonnull Boolean positiveMatch, @Nonnull Boolean caseSensitive) {
        return patternMatchingAdapter.matchRegex(fieldToMatch, regex, positiveMatch, caseSensitive);
    }

    @Nonnull
    @Override
    public Condition contains(@Nonnull Field<String> field, @Nonnull String pattern,
            @Nonnull Boolean positiveMatch, @Nonnull Boolean caseSensitive) {
        return patternMatchingAdapter.contains(field, pattern, positiveMatch, caseSensitive);
    }
}
