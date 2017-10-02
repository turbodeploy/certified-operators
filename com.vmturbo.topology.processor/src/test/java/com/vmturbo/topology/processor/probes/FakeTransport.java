package com.vmturbo.topology.processor.probes;

import java.util.concurrent.atomic.AtomicInteger;

import com.vmturbo.communication.AbstractTransport;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.ITransport;

/**
 * Class holds various usefull static methods for creating fake transports.
 */
public class FakeTransport {

    private static final AtomicInteger counter = new AtomicInteger();

    private FakeTransport() {}

    public static <S, R> TransportPair<S, R> createSymmetricTransport() {
        final SymmetricTransport<S, R> server = new SymmetricTransport<>();
        final SymmetricTransport<R, S> client = new SymmetricTransport<>(server);
        return new TransportPair<S, R>(server, client);
    }

    /**
     * Symmetric transport represents {@link ITransport} pair, where one transport transmits exactly
     * what another transport receives.
     *
     * @param <S> data type to send
     * @param <R> data type to receive
     */
    private static class SymmetricTransport<S, R> extends AbstractTransport<S, R> {

        private final int id = counter.getAndIncrement();
        private SymmetricTransport<R, S> counterPart;

        private SymmetricTransport() {}

        private SymmetricTransport(SymmetricTransport<R, S> counterPart) {
            this.counterPart = counterPart;
            counterPart.counterPart = this;
        }

        @Override
        protected void closeInternal() {
            counterPart.close();
        }

        @Override
        protected R createReadOnly(R originalMessage) {
            return originalMessage;
        }

        @Override
        protected void sendInternal(S messageToSend) throws CommunicationException,
                        InterruptedException {
            counterPart.notifyMessageReceived(messageToSend);
        }

        @Override
        public String toString() {
            return "transport-" + id;
        }
    }

    /**
     * A wrapper to hold 2 transports (sending and receiving) simultaneously.
     *
     * @param <S> the data, which server sends
     * @param <R> data, which server receives
     */
    public static class TransportPair<S, R> {
        private final ITransport<S, R> serverTransport;
        private final ITransport<R, S> clientTransport;

        private TransportPair(ITransport<S, R> serverTransport, ITransport<R, S> clientTransport) {
            this.serverTransport = serverTransport;
            this.clientTransport = clientTransport;
        }

        public ITransport<S, R> getServerTransport() {
            return serverTransport;
        }

        public ITransport<R, S> getClientTransport() {
            return clientTransport;
        }

    }
}
