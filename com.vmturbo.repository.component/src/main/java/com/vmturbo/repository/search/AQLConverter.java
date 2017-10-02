package com.vmturbo.repository.search;

/**
 * An interface for objects that can be converted to AQL.
 */
public interface AQLConverter {

    /**
     * The AQL string for the object.
     *
     * @return AQL.
     */
    String toAQLString();
}
