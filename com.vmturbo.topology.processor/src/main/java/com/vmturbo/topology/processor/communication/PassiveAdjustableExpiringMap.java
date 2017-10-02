package com.vmturbo.topology.processor.communication;

import java.time.Clock;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.collections4.map.AbstractMapDecorator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Derived from {@link org.apache.commons.collections4.map.PassiveExpiringMap}.
 *
 * Decorates a <code>Map</code> to remove expired entries once their expiration
 * time has been reached. Does not permit the insertion of {@code null} values.
 *
 * <p>When invoking methods that involve accessing the map contents (i.e
 * {@link #containsKey(Object)}, {@link #entrySet()}, etc.) this decorator
 * removes all expired entries prior to actually completing the invocation.
 * The check is made via the value's {@link ExpiringValue ::#expirationTime}
 * method and if it is a negative value or if it is greater than the current
 * time the value is retained, otherwise the value's onExpiration method is
 * called and it is removed from the map. <strong>Note that as a result,
 * of this check, map operations that usually are constant time operations
 * are linear time operations in maps of this type.</strong>
 *
 * <p>When the map expires a value, it will notify the value of the expiration
 * via a call to {@link ExpiringValue::#onExpiration()}.
 *
 * <p>This class differs from a {@link org.apache.commons.collections4.map.PassiveExpiringMap}
 * in that the expiration time for an entry is checked immediately prior to checking
 * for expiration rather than when the value is put in the map. This allows entry
 * expiration to be lengthened or renewed without removing and then re-adding the entry.
 * Further, entries from an existing map passed to the constructor are never expired
 * from a {@link org.apache.commons.collections4.map.PassiveExpiringMap}
 * while in an {@link PassiveAdjustableExpiringMap}, entries in a map passed to the
 * constructor will expire when the {@link ExpiringValue} determines its expiration time
 * has passed.
 *
 * <p><strong>Note that {@link PassiveAdjustableExpiringMap} is not synchronized and is not
 * thread-safe.</strong> If you wish to use this map from multiple threads
 * concurrently, you must use appropriate synchronization. The simplest approach
 * is to wrap this map using {@link java.util.Collections#synchronizedMap(Map)}.
 * Additionally, if the {@link ExpiringValue} concurrently accesses data in its
 * {@link ExpiringValue#expirationTime}, calls to the {@link ExpiringValue#expirationTime()}
 * and {@link ExpiringValue#onExpiration()} should be thread-safe. This class may
 * throw exceptions when accessed by concurrent threads without synchronization.
 *
 * @param <K> the type of the keys in the map
 * @param <V> the type of the values in the map
 */
@NotThreadSafe
public class PassiveAdjustableExpiringMap<K, V extends ExpiringValue>
    extends AbstractMapDecorator<K, V> {

    /**
     * The clock used to calculate expiration time.
     */
    private final Clock expirationClock;

    private final Logger logger = LogManager.getLogger();

    /**
     * Construct a map decorator that expires entries when their expiration time is exceeded.
     * Uses an expiration clock based on System UTC time.
     */
    public PassiveAdjustableExpiringMap() {
        this(new HashMap<>(), Clock.systemUTC());
    }

    /**
     * Construct a map decorator that decorates the given map and
     * expires entries when their expiration time is exceeded.
     * If there are entries in the map being decorated, they expire
     * in the same fashion as entries added via calls to {@link #put}.
     *
     * @param map the map to decorate, must not be null.
     * @param expirationClock The clock to use when calculating expiration times.
     * @throws NullPointerException if the map or expiringPolicy is null.
     */
    public PassiveAdjustableExpiringMap(final Map<K, V> map, final @Nonnull Clock expirationClock) {
        super(map);
        this.expirationClock = expirationClock;
    }

    /**
     * Normal {@link Map#clear()} behavior with the addition of clearing all
     * expiration entries as well.
     */
    @Override
    public void clear() {
        super.clear();
    }

    /**
     * All expired entries are removed from the map prior to determining the
     * contains result.
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(final Object key) {
        removeAllExpired(now());
        return super.containsKey(key);
    }

    /**
     * All expired entries are removed from the map prior to determining the
     * contains result.
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(final Object value) {
        removeAllExpired(now());
        return super.containsValue(value);
    }

    /**
     * All expired entries are removed from the map prior to returning the entry set.
     * {@inheritDoc}
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        removeAllExpired(now());
        return super.entrySet();
    }

    /**
     * All expired entries are removed from the map prior to returning the entry value.
     * {@inheritDoc}
     */
    @Override
    public V get(final Object key) {
        removeAllExpired(now());
        return super.get(key);
    }

    /**
     * All expired entries are removed from the map prior to determining if it is empty.
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        removeAllExpired(now());
        return super.isEmpty();
    }

    /**
     * Determines if the given expiration time is less than <code>now</code>.
     *
     * @param now the time in milliseconds used to compare against the
     *        expiration time.
     * @param expirationTime the expiration time value retrieved from
     *        the {@link ExpiringValue}.
     * @return <code>true</code> if <code>expirationTimeObject</code> is &ge; 0
     *         and <code>expirationTimeObject</code> &lt; <code>now</code>.
     *         <code>false</code> otherwise.
     */
    private boolean isExpired(final long now, final long expirationTime) {
        return expirationTime >= 0 && now >= expirationTime;
    }

    /**
     * All expired entries are removed from the map prior to returning the key set.
     * {@inheritDoc}
     */
    @Override
    public Set<K> keySet() {
        removeAllExpired(now());
        return super.keySet();
    }

    /**
     * Add the given key-value pair to this map.
     *
     * @throws NullPointerException if the value is null.
     *
     * <p>{@inheritDoc}
     */
    @Override
    public V put(final K key, final V value) {
        if (value == null) {
            throw new NullPointerException();
        }
        return super.put(key, value);
    }

    /**
     * Add all the given key-value pairs to this map. The value cannot be null.
     *
     * @throws NullPointerException if any of the values in the map is null.
     *
     * <p>{@inheritDoc}
     */
    @Override
    public void putAll(@Nonnull final Map<? extends K, ? extends V> mapToCopy) {
        for (final Entry<? extends K, ? extends V> entry : mapToCopy.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Removes all entries in the map whose expiration time is less than
     * <code>now</code>. The exceptions are entries with negative expiration
     * times; those entries are never removed.
     *
     * @see #isExpired(long, long)
     * @param now The current time.
     */
    private void removeAllExpired(final long now) {
        final Iterator<Entry<K, V>> iter = super.entrySet().iterator();
        while (iter.hasNext()) {
            final Entry<K, V> entry = iter.next();
            final long expirationTime = entry.getValue().expirationTime();
            if (isExpired(now, expirationTime)) {
                logger.debug(() -> "Expiring value " + entry.getValue() + "; now: " + now +
                        "expirationTime: " + expirationTime);
                entry.getValue().onExpiration();
                iter.remove();
            }
        }
    }

    private long now() {
        return expirationClock.millis();
    }

    /**
     * All expired entries are removed from the map prior to returning the size.
     * {@inheritDoc}
     */
    @Override
    public int size() {
        removeAllExpired(now());
        return super.size();
    }

    /**
     * All expired entries are removed from the map prior to returning the value collection.
     * {@inheritDoc}
     */
    @Override
    public Collection<V> values() {
        removeAllExpired(now());
        return super.values();
    }

    /**
     * Get the clock to be used for expiration timeout calculations.
     * @return The clock used for expiration timeout calculations.
     */
    public Clock getExpirationClock() {
        return expirationClock;
    }
}
