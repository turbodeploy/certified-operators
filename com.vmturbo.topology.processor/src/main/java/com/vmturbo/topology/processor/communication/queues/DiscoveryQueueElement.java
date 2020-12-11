package com.vmturbo.topology.processor.communication.queues;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryBundle;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Element with all the pieces necessary to run a discovery.  It contains the target to be
 * discovered, a BiFunction that starts a discovery and returns the {@link Discovery} object
 * representing that discovery and calls an IntSupplier when the discovery is completed.  This
 * IntSupplier is used to release the permit with the discovery is complete.
 */
public class DiscoveryQueueElement implements IDiscoveryQueueElement {

    private static final Logger logger = LogManager.getLogger(DiscoveryQueueElement.class);

    private final Target target;

    private final DiscoveryType discoveryType;

    private final Function<Runnable, DiscoveryBundle> prepareDiscoveryInformation;

    private final LocalDateTime queuedTime;

    private Discovery discovery;

    private boolean runImmediately;

    private final BiConsumer<Discovery, Exception> errorHandler;

    /**
     * indicates whether we've tried to launch the discovery or not.
     */
    private final AtomicBoolean discoveryAttempted = new AtomicBoolean(false);

    /**
     * Create a {@link DiscoveryQueueElement}.
     *
     * @param target the {@link Target} being discovered.
     * @param discoveryType the DiscoveryType of the discovery to be performed.
     * @param prepareDiscoveryInformation a function that takes a Runnable and returns a
     * DiscoveryBundle object.
     * @param errorHandler method to invoke if execution of discovery encounters and exception.
     * @param runImmediately boolean indicating whether this element should wait in the queue or
     * go to the front.
     */
    public DiscoveryQueueElement(@Nonnull Target target, @Nonnull DiscoveryType discoveryType,
            @Nonnull Function<Runnable, DiscoveryBundle> prepareDiscoveryInformation,
            @Nonnull BiConsumer<Discovery, Exception> errorHandler,
            boolean runImmediately) {
        this.target = target;
        this.discoveryType = discoveryType;
        this.prepareDiscoveryInformation = prepareDiscoveryInformation;
        this.errorHandler = errorHandler;
        this.runImmediately = runImmediately;
        queuedTime = LocalDateTime.now();
    }

    /**
     * Create a {@link DiscoveryQueueElement}.
     *
     * @param target the {@link Target} being discovered.
     * @param discoveryType the DiscoveryType of the discovery to be performed.
     * @param prepareDiscoveryInformation a function that takes a Runnable and returns a
     * DiscoveryBundle object.
     * @param errorHandler method to call if an exception is encountered running a discovery.
     */
    public DiscoveryQueueElement(@Nonnull Target target, @Nonnull DiscoveryType discoveryType,
            @Nonnull Function<Runnable, DiscoveryBundle>  prepareDiscoveryInformation,
            @Nonnull BiConsumer<Discovery, Exception> errorHandler) {
        this(target, discoveryType, prepareDiscoveryInformation, errorHandler, false);
    }

    @Override
    public Target getTarget() {
        return target;
    }

    @Override
    public LocalDateTime getQueuedTime() {
        return queuedTime;
    }

    @Override
    public Discovery performDiscovery(
            @Nonnull Function<DiscoveryBundle, Discovery> discoveryMethod,
            @Nonnull Runnable returnPermit) {
        try {
            logger.debug("performDiscovery called for target {}({})", target.getId(),
                    discoveryType);
            DiscoveryBundle discoveryBundle = prepareDiscoveryInformation.apply(returnPermit);
            logger.debug("DiscoveryBundle is {}", discoveryBundle);
            if (discoveryBundle != null) {
                discovery = discoveryMethod.apply(discoveryBundle);
                Exception ex = discoveryBundle.getException();
                if (discovery != null && ex != null) {
                    errorHandler.accept(discovery, ex);
                }
            } else {
                // if discoveryBundle is null, no discovery can be run so run the runnable to
                // return the permit.
                returnPermit.run();
            }
        } finally {
            logger.debug("Discovery created for target {}. Notifying all.", target.getId());
            synchronized (discoveryAttempted) {
                discoveryAttempted.set(true);
                discoveryAttempted.notifyAll();
            }
        }
        logger.debug("Discovery: {}", discovery == null ? "discovery creation failed"
                : discovery);
        return discovery;
    }

    @Nullable
    @Override
    public Discovery getDiscovery(long timeoutMillis) throws InterruptedException {
        synchronized (discoveryAttempted) {
            final long startTime = System.currentTimeMillis();
            while (!discoveryAttempted.get()) {
                logger.debug("Waiting for discovery to be launched for target {}({}).",
                        target.getId(), discoveryType);
                discoveryAttempted.wait(timeoutMillis);
                if (timeoutMillis > 0 && System.currentTimeMillis() - startTime > timeoutMillis) {
                    logger.info("Returning empty discovery due to timeout waiting for queued "
                            + "discovery to be run for target {}({}).", target.getId(),
                            discoveryType);
                    break;
                }
            }
        }
        logger.debug("Discovery attempted. Returning discovery: {}", discovery == null
                ? "discovery creation failed"
                : discovery.getId());
        return discovery;
    }

    @Override
    public boolean runImmediately() {
        return runImmediately;
    }

    @Override
    public DiscoveryType getDiscoveryType() {
        return discoveryType;
    }

    @Override
    public String toString() {
        return target.getProbeId() + "::" + target.getDisplayName() + "(" + discoveryType + ")" + "::" + queuedTime
                + (runImmediately ? "::runImmediately" : "");
    }

    @Override
    public void setRunImmediately(boolean newValue) {
        runImmediately = newValue;
    }

    @Override
    public int compareTo(@NotNull IDiscoveryQueueElement other) {
        if (runImmediately == other.runImmediately()) {
            return queuedTime.compareTo(other.getQueuedTime());
        } else if (runImmediately) {
            return -1;
        } else {
            return 1;
        }
    }
}
