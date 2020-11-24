package com.vmturbo.integrations.intersight;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.cisco.intersight.client.ApiClient;
import com.cisco.intersight.client.ApiException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.connector.intersight.IntersightDefaultQueryParameters;

/**
 * Class of IntersightApiQuery.
 *
 * @param <T> type of the UCS instance to be queried
 * @param <L> list type of the UCS instance to be queried
 */
public abstract class IntersightApiQuery<T, L> {
    private static final String INTERSIGHT_TRACE_ID = "x-starship-traceid";
    protected final Logger logger = LogManager.getLogger();
    protected String filter = IntersightDefaultQueryParameters.$filter;
    protected String orderBy = IntersightDefaultQueryParameters.$orderby;
    protected int top = IntersightDefaultQueryParameters.$top;
    protected int skip = IntersightDefaultQueryParameters.$skip;
    protected String select = IntersightDefaultQueryParameters.$select;
    protected String expand = IntersightDefaultQueryParameters.$expand;
    protected String apply = IntersightDefaultQueryParameters.$apply;
    protected boolean count = IntersightDefaultQueryParameters.$count;
    protected String inlineCount = IntersightDefaultQueryParameters.$inlinecount;
    protected String at = IntersightDefaultQueryParameters.$at;
    protected String tags = IntersightDefaultQueryParameters.$tags;

    /**
     * Query instances through API.
     *
     * @param client Intersight API Client
     * @return api query instances
     * @throws ApiException api exception thrown
     */
    protected abstract @Nullable L queryInstances(ApiClient client) throws ApiException;

    /**
     * Get the query instance type.
     *
     * @return query instance type
     */
    protected abstract @Nonnull String getInstanceType();

    /**
     * Convert the response.
     *
     * @param l response to be converted
     * @return converted response
     */
    protected abstract @Nullable List<T> convertResponse(@Nonnull L l);

    /**
     * Get a list of instances from api query or throw an {@link ApiException} if encountered.
     *
     * @param client API client
     * @return list of instances from api query
     * @throws ApiException if encountered while fetching the list of instances
     */
    protected @Nonnull List<T> getInstanceListOrElseThrow(@Nonnull ApiClient client) throws ApiException {
        L response;
        try {
            response = queryInstances(client);
        } catch (ApiException e) {
            logger.warn("Failed to query {} instances. TraceId: {}",
                    getInstanceType(),
                    e.getResponseHeaders()
                            == null ? null : e.getResponseHeaders().get(INTERSIGHT_TRACE_ID));
            throw e;
        }
        if (response == null) {
            return Collections.emptyList();
        }
        final List<T> instances = convertResponse(response);
        return instances != null ? instances : Collections.emptyList();
    }

    /**
     * Get a list of instances from api query.  An empty list will be returned if an
     * {@link ApiException} is encountered.
     *
     * @param client API client
     * @return list of instances from api query
     */
    protected @Nonnull List<T> getInstanceList(@Nonnull ApiClient client) {
        try {
            return getInstanceListOrElseThrow(client);
        } catch (ApiException e) {
            return Collections.emptyList();
        }
    }

    /**
     * Get the filter for this query.
     *
     * @return the filter for this query
     */
    protected @Nullable String getFilter() {
        return filter;
    }

    /**
     * get all api query instances.  If an {@link ApiException} is encountered, a partial list
     * will be returned up to the point of exception.
     *
     * @param client Intersight API client
     * @return all api query instances
     */
    public @Nonnull List<T> getAllQueryInstances(@Nonnull ApiClient client) {
        final List<T> allInstances = new ArrayList<>();
        boolean doQuery = true;
        while (doQuery) {
            final List<T> instances = getInstanceList(client);
            allInstances.addAll(instances);

            if (instances.size() == top) {
                skip += top;
                doQuery = true;
            } else {
                doQuery = false;
            }
        }

        logger.debug(" Number of {} instance queried with filter {} : {} ", getInstanceType(),
                getFilter(), allInstances.size());
        return allInstances;
    }

    /**
     * get all api query instances or throw an {@link ApiException} if one is encountered.
     *
     * @param client Intersight API client
     * @return all api query instances
     * @throws ApiException if encountered while fetching the list of instances
     */
    public @Nonnull List<T> getAllQueryInstancesOrElseThrow(@Nonnull ApiClient client) throws ApiException {
        final List<T> allInstances = new ArrayList<>();
        while (true) {
            final List<T> instances = getInstanceListOrElseThrow(client);
            allInstances.addAll(instances);

            if (instances.size() != top) {
                break;
            }
            skip += top;
        }

        logger.debug(" Number of {} instance queried with filter {} : {} ", getInstanceType(),
                getFilter(), allInstances.size());
        return allInstances;
    }
}
