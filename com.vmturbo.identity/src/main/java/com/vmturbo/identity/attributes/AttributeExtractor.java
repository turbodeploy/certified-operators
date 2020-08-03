package com.vmturbo.identity.attributes;

import javax.annotation.Nonnull;

/**
 * Given an item of type T, return the unique com.vmturbo.identity.attributes that will be used in determing
 * equality with a different item of type T.
 *
 * The particular com.vmturbo.identity.attributes, and as well the accessors for those com.vmturbo.identity.attributes, will be unique
 * to each implementation of this interface.
 *
 * @param <T> This is the type of item from which com.vmturbo.identity.attributes will be extracted
 **/
public interface AttributeExtractor<T> {
    /**
     * Query the 'item' passed in, retrieving a set of key/value com.vmturbo.identity.attributes and store those
     * in a newly created {@link IdentityMatchingAttributes}.
     *
     * @param item the item whose com.vmturbo.identity.attributes will be extracted
     * @return a new model that is representing the instance of {@code item}
     */
    @Nonnull
    IdentityMatchingAttributes extractAttributes(@Nonnull T item);
}
