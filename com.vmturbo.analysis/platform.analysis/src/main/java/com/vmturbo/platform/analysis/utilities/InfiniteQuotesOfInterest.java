package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityQuote;

/**
 * {@link InfiniteQuotesOfInterest}, given a collection of {@link Quote}s for a {@link ShoppingList},
 * determines which of those {@link Quote}s is most relevant and can be used to explain those
 * {@link Quote}s.
 * Will attempt to explain ALL non-{@link CommodityQuote}s.
 * For {@link CommodityQuote}s, will find the most relevant {@link Quote} for
 * each {@link CommoditySpecification} found to be insufficient in any of the quotes.
 * For example, if the shopping list wants to buy 1000 units of Commodity "Foo",
 * and it got two quotes, one with availableQuantity=500, and one with availableQuantity=600,
 * the one with availableQuantity=600 is more relevant because it is closer to the
 * requested amount, so only explain that one but not the 500 one because it is not interesting
 * in light of the 600 quote.
 */
public class InfiniteQuotesOfInterest {
    private final List<Quote> nonCommodityQuotes;
    private final ShoppingList shoppingList;
    private final Map<CommoditySpecification, IndividualCommodityQuote> individualCommodityQuotes;


    /**
     * Constructor.
     *
     * @param quoteTracker A {@QuoteTracker} which contains infinite quotes that needs to be explained.
     */
    public InfiniteQuotesOfInterest(@Nonnull final QuoteTracker quoteTracker) {
        nonCommodityQuotes = new ArrayList<>();
        shoppingList = quoteTracker.getShoppingList();
        individualCommodityQuotes = new HashMap<>();

        quoteTracker.getInfiniteQuotesToExplain().stream()
            .forEach(quote -> {
            if (quote instanceof Quote.CommodityQuote) {
                // For each individual commodity, the quote of interest is the one with the most available
                // quantity.
                final CommodityQuote cq = (CommodityQuote)quote;
                cq.getInsufficientCommodities().forEach(insufficientCommodity -> {
                    final CommoditySpecification commodity = insufficientCommodity.commodity;
                    final IndividualCommodityQuote interestingQuote = individualCommodityQuotes.get(commodity);
                    if (interestingQuote == null) {
                        individualCommodityQuotes.put(commodity, new IndividualCommodityQuote(quote,
                            insufficientCommodity.availableQuantity));
                    } else if (interestingQuote.availableQuantity < insufficientCommodity.availableQuantity) {
                        // Replace the seller of interest for this commodity
                        individualCommodityQuotes.put(commodity, new IndividualCommodityQuote(
                            quote, insufficientCommodity.availableQuantity));
                    }
                });
            } else {
                // Explain all non-commodity quotes.
                nonCommodityQuotes.add(quote);
            }
        });
    }

    /**
     * Returns a list of quotes associated with non CommodityQuote.
     *
     * @return a list of quotes associated with non CommodityQuote.
     */
    public List<Quote> getNonCommodityQuotes() {
        return nonCommodityQuotes;
    }

    /**
     * Returns a map of {@link CommoditySpecification} to {@link IndividualCommodityQuote}.
     *
     * @return a map of {@link CommoditySpecification} to {@link IndividualCommodityQuote}.
     */
    public Map<CommoditySpecification, IndividualCommodityQuote> getIndividualCommodityQuotes() {
        return individualCommodityQuotes;
    }

    /**
     * A small helper class that pairs a quote with an available quantity for an individual commodity.
     * Used by {@link InfiniteQuotesOfInterest}.
     */
    @Immutable
    public static class IndividualCommodityQuote {
        /**
         * A quote object.
         */
        public final Quote quote;
        /**
         * The max quantity available given by the seller of the quote.
         */
        public final double availableQuantity;

        /**
         * Constructor.
         *
         * @param quote The quote.
         * @param availableQuantity The max quantity available given by the seller of the quote.
         */
        public IndividualCommodityQuote(@Nullable final Quote quote,
                                        final double availableQuantity) {
            this.quote = Objects.requireNonNull(quote);
            this.availableQuantity = availableQuantity;
        }
    }
}
