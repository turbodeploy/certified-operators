package com.vmturbo.identity.attributes;

import javax.annotation.Nonnull;

/**
 * Given an item of type T, return the unique attributes that will be used in determing
 * equality with a different item of type T.
 *
 * The particular attributes, and as well the accessors for those attributes, will be unique
 * to each implementation of this interface.
 *
 * @param <T> This is the type of item from which attributes will be extracted
 **/
public interface AttributeExtractor<T> {
    /**
     * Query the 'item' passed in, retrieving a set of key/value attributes and store those
     * in a newly created {@link IdentityMatchingAttributes}.
     *
     * @param item the item whose attributes will be extracted
     * @return a new {@link IdentityMatchingAttributes} populated with the desired attributes
     * extracted from the item
     */
    @Nonnull
    IdentityMatchingAttributes extractAttributes(@Nonnull T item);
}
