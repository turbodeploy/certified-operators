package com.vmturbo.platform.analysis.utilities;

import com.google.common.collect.ImmutableList;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.javatuples.Triplet;

import java.util.HashMap;

/**
 * A caching structure that is used to save quotes for a {@link ShoppingList} from a provider
 * hosting a specific set of customers. This class also has methods for storing, updating and
 * retrieving of the entries.
 *
 */
public final class QuoteCache extends HashMap<Triplet<@NonNull ShoppingList/*buyer*/,
                                                    @NonNull Trader/*seller*/,
                                                    @NonNull ImmutableList<@NonNull ShoppingList>>/*seller state*/,
                                            @NonNull MutableQuote> {

} // end QuoteCache class
