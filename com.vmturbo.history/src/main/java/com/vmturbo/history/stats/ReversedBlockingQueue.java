/**
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats;

import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

/**
 * {@link ReversedBlockingQueue} blocking queue which is working as LIFO, i.e. element placed last
 * will be taken from it first. It is useful in case we need to change the order of task execution
 * in the thread pool when the longest tasks added last. Default {@link java.util.Queue}
 * implementation used in {@link java.util.concurrent.ThreadPoolExecutor} provided by {@link
 * java.util.concurrent.Executors} factory will execute tasks in the order they were placed into the
 * {@link java.util.Queue}. This will cause small inefficiency in case thread pool with fixed amount
 * of thread is using, because the longest tasks will not be even started by the thread pool. This
 * {@link java.util.Queue} implementation allows to reduce inefficiency for this case.
 *
 * @param <E> the type of elements held in this collection
 */
public class ReversedBlockingQueue<E> extends AbstractQueue<E>
                implements BlockingQueue<E>, Serializable {
    private static final long serialVersionUID = 1802017725587941708L;
    private final BlockingDeque<E> q = new LinkedBlockingDeque<>();

    @Override
    public boolean add(@Nonnull E e) {
        q.addFirst(e);
        return true;
    }

    @Override
    public boolean offer(@Nonnull E e) {
        return q.offerFirst(e);
    }

    @Override
    public void put(@Nonnull E e) throws InterruptedException {
        q.putFirst(e);
    }

    @Override
    public boolean offer(E e, long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        return q.offerFirst(e, timeout, unit);
    }

    @Nonnull
    @Override
    public E take() throws InterruptedException {
        return q.takeFirst();
    }

    @Override
    public E poll(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        return q.pollFirst(timeout, unit);
    }

    @Override
    public int remainingCapacity() {
        return q.remainingCapacity();
    }

    @Override
    public E poll() {
        return q.pollFirst();
    }

    @Override
    public E remove() {
        return q.removeFirst();
    }

    @Override
    public E peek() {
        return q.peekFirst();
    }

    @Override
    public E element() {
        return q.getFirst();
    }

    @Override
    public void clear() {
        q.clear();
    }

    @Override
    public int size() {
        return q.size();
    }

    @Override
    public boolean isEmpty() {
        return q.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return q.contains(o);
    }

    @Override
    public int drainTo(@Nonnull Collection<? super E> c) {
        return q.drainTo(c);
    }

    @Override
    public int drainTo(@Nonnull Collection<? super E> c, int maxElements) {
        return q.drainTo(c, maxElements);
    }

    @Override
    public boolean remove(Object o) {
        return q.remove(o);
    }

    @Nonnull
    @Override
    public Iterator<E> iterator() {
        return q.iterator();
    }

    @Nonnull
    @Override
    public Object[] toArray() {
        return q.toArray();
    }

    @Nonnull
    @Override
    public <T> T[] toArray(@Nonnull T[] a) {
        return q.toArray(a);
    }

    @Override
    public String toString() {
        return q.toString();
    }

    @Override
    public boolean containsAll(@Nonnull Collection<?> c) {
        return q.containsAll(c);
    }

    @Override
    public boolean removeAll(@Nonnull Collection<?> c) {
        return q.removeAll(c);
    }

    @Override
    public boolean retainAll(@Nonnull Collection<?> c) {
        return q.retainAll(c);
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        q.forEach(action);
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {

        return q.removeIf(filter);
    }

    @Override
    public Spliterator<E> spliterator() {
        return q.spliterator();
    }

    @Override
    public Stream<E> stream() {
        return q.stream();
    }

    @Override
    public Stream<E> parallelStream() {
        return q.parallelStream();
    }

}
