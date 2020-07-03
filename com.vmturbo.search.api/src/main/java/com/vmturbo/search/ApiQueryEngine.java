package com.vmturbo.search;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.searchquery.EntityCountRequestApiDTO;
import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.dto.searchquery.GroupCountRequestApiDTO;
import com.vmturbo.api.dto.searchquery.GroupQueryApiDTO;
import com.vmturbo.api.dto.searchquery.SearchCountRecordApiDTO;
import com.vmturbo.api.dto.searchquery.SearchQueryRecordApiDTO;
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Read only from search(extractor) database.
 *
 * <p>Responsible for mapping {@link EntityQueryApiDTO} request into queries, fetching data,
 *    and mapping back into response objects i.e {@link SearchQueryRecordApiDTO} for entities </p>
 **/
public class ApiQueryEngine implements IApiQueryEngine {

    /**
     * A database endpoint configuration.
     */
    private final DbEndpoint readonlyDbEndpoint;

    /**
     * A feature flag, if true then search features are enabled.
     */
    private final boolean enableSearchApi;

    /**
     * A factory for constructing search queries of different types (entity, group).
     */
    private QueryFactory queryFactory;

    /**
     * Construct an ApiQueryEngine.
     *
     * @param readonlyDbEndpoint a database endpoint configuration
     * @param enableSearchApi a feature flag, if true then search features are enabled
     */
    public ApiQueryEngine(@Nonnull DbEndpoint readonlyDbEndpoint, boolean enableSearchApi) {
        this.readonlyDbEndpoint = Objects.requireNonNull(readonlyDbEndpoint);
        this.enableSearchApi = enableSearchApi;
    }

    @Override
    public SearchQueryPaginationResponse processEntityQuery(@Nonnull final EntityQueryApiDTO request)
            throws UnsupportedDialectException, SQLException {
        if (!enableSearchApi) {
            throw new UnsupportedOperationException("Search API is not yet enabled!");
        }
        return getQueryFactory().performEntityQuery(request);
    }

    @Override
    public SearchQueryPaginationResponse processGroupQuery(@Nonnull final GroupQueryApiDTO request)
        throws UnsupportedDialectException, SQLException {
        if (!enableSearchApi) {
            throw new UnsupportedOperationException("Search API is not yet enabled!");
        }
        return getQueryFactory().performGroupQuery(request);
    }

    @Override
    public List<SearchCountRecordApiDTO> countEntites(
            final EntityCountRequestApiDTO request) throws Exception {
        if (!enableSearchApi) {
            throw new UnsupportedOperationException("Search API is not yet enabled!");
        }
        return getQueryFactory().performEntityCount(request);
    }

    @Override
    public List<SearchCountRecordApiDTO> countGroups(
            final GroupCountRequestApiDTO request) throws Exception {
        if (!enableSearchApi) {
            throw new UnsupportedOperationException("Search API is not yet enabled!");
        }
        return getQueryFactory().performGroupCount(request);
    }

    /**
     * Initializes the API query factory from the {@link DbEndpoint}.
     *
     * @return a factory for constructing API database queries
     * @throws UnsupportedDialectException when the dialect is not supported
     * @throws SQLException when a problem has occurred connecting to the database
     */
    private QueryFactory getQueryFactory() throws UnsupportedDialectException, SQLException {
        if (this.queryFactory == null) {
            try {
                this.queryFactory = new QueryFactory(readonlyDbEndpoint.dslContext());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return queryFactory;
    }

}
