package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Wrapper for storing prices per account.
 * Allows to access Prices
 *
 * @param <T> destination price type
 * @param <S> source price type
 */
public class PriceHolder<T, S> {

    private static final Logger logger = LogManager.getLogger();

    private final Map<Long, Map<Long, T>> priceMap;
    private final Map<Long, Long> priceTableKeyOidByBusinessAccountOid;

    /**
     * Create an {@link PriceHolder} instance.
     *
     * @param priceTables price table mapping
     * @param priceTableKeyOidByBusinessAccountOid PriceTable oid to BA oid mapping
     * @param transform function to transform {@link S} into {@link T}
     */
    public PriceHolder(Map<Long, S> priceTables,
            Map<Long, Long> priceTableKeyOidByBusinessAccountOid,
            Function<S, Map<Long, T>> transform) {

        final ImmutableMap.Builder<Long, Map<Long, T>> builder = ImmutableMap.builder();
        //create Price oid -> Spec oid -> Price Table mapping
        for (Entry<Long, S> entry : priceTables.entrySet()) {
            builder.put(entry.getKey(), ImmutableMap.copyOf(transform.apply(entry.getValue())));
        }
        this.priceMap = builder.build();
        logger.debug("populate price map for {}  sizes=[{}]", () -> priceTables.values()
                .stream()
                .findFirst()
                .map(e -> e.getClass().getSimpleName()), () -> priceMap.entrySet()
                .stream()
                .map(e -> String.format("%s: %s", e.getKey(), e.getValue().size()))
                .collect(Collectors.joining(", ")));
        this.priceTableKeyOidByBusinessAccountOid = priceTableKeyOidByBusinessAccountOid;
    }

    /**
     * Get Price by BA oid and Price oid.
     *
     * @param baOid BA oid
     * @param specOid Price oid
     * @return the Price
     */
    public T get(long baOid, long specOid) {
        final Long keyOid = priceTableKeyOidByBusinessAccountOid.get(baOid);
        if (priceMap.containsKey(keyOid)) {
            return priceMap.get(keyOid).get(specOid);
        }
        return null;
    }
}
