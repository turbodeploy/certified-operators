package com.vmturbo.platform.analysis.economy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

/**
 * An acceleration structure that maps keys of integer to a postings list which is
 * a list of traders.
 *
 * Useful when trying to match sellers to markets. Do NOT mutate the basket associated with
 * a value after it has been added to the index otherwise results are undefined. If
 * it is necessary to mutate a value's basket, first remove it and then re-add it to the index.
 *
 * The index contains no locks and is NOT thread safe.
 *
 * The concept of an inverted index comes from information retrieval systems.
 * See for example http://nlp.stanford.edu/IR-book/html/htmledition/an-example-information-retrieval-problem-1.html#963
 * which also includes many guidelines for how to speed up information retrieval.
 */
class InvertedIndex implements Serializable {

    private final Economy economy;
    private final Map<Integer, PostingsList> index = new HashMap<>();
    private final int minimalScanStopThreshold;

    /**
     * Create a new inverted index.
     *
     * @param economy The economy associated with the index.
     * @param minimalScanStopThreshold The number of items in a postings list at or below
     *        which the scan for a minimal postings list can stop. Used to early-exit minimal
     *        postings scans when there are many commodities in a basket. This number should be
     *        >= 0.
     */
    public InvertedIndex(final Economy economy,
                         final int minimalScanStopThreshold) {
        Preconditions.checkNotNull(economy);
        Preconditions.checkArgument(minimalScanStopThreshold >= 0);

        this.economy = economy;
        this.minimalScanStopThreshold = minimalScanStopThreshold;
    }

    /**
     * Add a new value to the inverted index. Does NOT check if the value has already been added
     * to the inverted index.
     *
     * It is possible to add a value multiple times to the index. It is up to the caller
     * to avoid insertion of duplicates if that is the desired behavior.
     *
     * For a value with k commodities in its basket, this operation takes O(k) time.
     *
     * @param seller The value to add to the index.
     */
    public void add(final Trader seller) {
        // Add the market to the postings list for every commodity it contains
        for (CommoditySpecification specification : seller.getBasketSold()) {
            final PostingsList postings = index.computeIfAbsent(specification.getType(), type -> new PostingsList());
            postings.add(seller);
        }
    }

    /**
     * Returns the size of the index. The size of the index is defined as the number of keys in the index.
     *
     * @return The number of keys in the index.
     */
    public int indexSize() {
        return index.size();
    }

    /**
     * Count the total number of values in the index.
     *
     * @return The number of values in the index.
     */
    public int valueCount() {
        return index.values().stream()
            .collect(Collectors.reducing(0, ArrayList::size, Integer::sum));
    }

    /**
     * Get the minimal scan stop threshold.
     *
     * @return The number of items in a postings list at or below which the scan
     * for a minimal postings list can stop. Used to early-exit minimal postings
     * scans when there are many commodities in a basket.
     */
    public int getMinimalScanStopThreshold() {
        return minimalScanStopThreshold;
    }

    /**
     * Clear all keys and values from the index
     */
    public void clear() {
        index.clear();
    }

    /**
     * Remove a value from the inverted index.
     *
     * For an index containing V values, this operation takes O(V) time because
     * {@link CommoditySpecification}s in a {@link Basket} are unique.
     *
     * Note that it is impossible to remove a seller in the economy from
     * being associated with the empty basket because by definition all sellers
     * are in the empty basket.
     *
     * @param seller The value to remove from the index
     * @return The number of times the sellers was removed from the index. 0 if never removed.
     */
    public int remove(final Trader seller) {
        int numRemoved = 0;

        for (CommoditySpecification specification : seller.getBasketSold()) {
            final PostingsList postings = index.get(specification.getType());
            if (postings != null) {
                numRemoved += postings.remove(seller) ? 1 : 0;
            }
        }

        return numRemoved;
    }

    /**
     * Get the stream of traders in the economy that satisfy a given basket.
     *
     * If the basket is empty, returns all traders in the economy because all traders
     * may satisfy a market with an empty basket.
     *
     * For an economy with T traders in it, the method is guaranteed to run in O(T) time.
     * In practice, the method may be reasonably expected to run in amortized constant
     * time if run against the basket for every market in the economy. For an explanation
     * of why this is, see
     * https://vmturbo.atlassian.net/wiki/pages/viewpage.action?pageId=170271654
     *
     * @param basket The basket whose commodities should be checked.
     * @return A stream of all traders in the economy that satisfy the given basket.
     */
    public Stream<Trader> getSatisfyingTraders(final Basket basket) {
        return getMinimal(basket).stream()
            .filter(trader -> basket.isSatisfiedBy(trader.getBasketSold()));
    }

    /**
     * Get the list of postings of minimum size that contain any commodity in
     * the given basket.
     *
     * This call is guaranteed to return either the minimal list of postings for
     * a commodity in the basket or a list for a commodity in the basket of
     * size <= {@code minimalScanStopThreshold}.
     *
     * If the basket is empty, returns all traders in the economy because all traders
     * may satisfy a market with an empty basket.
     *
     * If there are no postings for any commodity in a non-empty basket, returns an empty list.
     *
     * For a basket with k commodities in it, this method runs in O(k) time.
     *
     * Should be used to determine the subset of traders to scan as possible sellers
     * in a market associated with the given basket.
     *
     * @param basket The basket whose commodities should be checked.
     * @return the list of postings of minimum size that contain any commodity in
     * the given basket.
     */
    private List<Trader> getMinimal(final Basket basket) {
        if (basket.isEmpty()) {
            return economy.getTraders();
        }

        Optional<List<Trader>> smallestMatchingPostings = Optional.empty();
        int minPostingsSize = Integer.MAX_VALUE;

        for (CommoditySpecification specification : basket) {
            final PostingsList postings = index.get(specification.getType());
            if (postings == null) {
                // No values for some commodity in the basket, so the empty list is the minimal list.
                return Collections.emptyList();
            }

            final int postingsSize = postings.size();
            if (postingsSize < minimalScanStopThreshold) {
                // Early-exit because we've already found a sufficiently small list to scan.
                // Given we cannot tightly bound the number of commodity specifications in a bucket
                // it is better to early exit when we find a sufficiently small set of postings we
                // can then exhaustively scan for satisfiability rather than ensuring we
                // always get the smallest set of postings.
                return postings;
            }

            if (postingsSize < minPostingsSize) {
                minPostingsSize = postingsSize;
                smallestMatchingPostings = Optional.of(postings);
            }
        }

        return smallestMatchingPostings.orElse(Collections.emptyList());
    }

    /**
     * A wrapper around a list of the type of values stored in the index.
     */
    private class PostingsList extends ArrayList<Trader> {}
}
