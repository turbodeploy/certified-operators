package com.vmturbo.auth.api;

/**
 * The STL pair implementation
 */
public class Pair<T1, T2> {

    /**
     * The first element
     */
    public final T1 first;

    /**
     * The second element
     */
    public final T2 second;

    /**
     * Constructs pair of objects
     *
     * @param first  The first element
     * @param second The second element
     */
    public Pair(final T1 first, final T2 second) {
        this.first = first;
        this.second = second;
    }
}


