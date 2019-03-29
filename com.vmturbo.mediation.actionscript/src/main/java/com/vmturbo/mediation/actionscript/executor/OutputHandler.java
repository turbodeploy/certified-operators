package com.vmturbo.mediation.actionscript.executor;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.util.Arrays;
import java.util.Observable;
import java.util.Queue;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class to collect command output sent to a piped output stream connected to an SSH channel's
 * stdout or stderr.
 *
 * <p>We collect and save only the most recent n lines of output, plus a final line for which we
 * have not yet received a line terminator.</p>
 *
 * <p>We operate in new thread so that we're always "live" and will eventually consume any available
 * output, so as to avoid locking up the overall execution waiting for buffers to be drained.</p>
 *
 * <p>This class implements {@link Observable} and notifies registered observers whenever new output
 * has been collected.</p>
 */
class OutputHandler extends Observable implements AutoCloseable {
    private static Logger logger = LogManager.getLogger(OutputHandler.class);

    // the thread that collects output
    private final Thread handler;
    // any output for which a line terminator has not yet been received
    private final StringBuffer partialLine = new StringBuffer();
    // name for this output handler (for logging only)
    private final String name;
    // FIFO queue of complete (i.e. terminated) lines of output
    private Queue<String> linesContainer = null;

    /**
     * Create a new output handler.
     * @param out a {@link PipedOutputStream} on which the output we are to collect will be written
     * @param maxOutputLines maximum number of lines of output to retain
     */
    OutputHandler(final @Nonnull PipedOutputStream out, final int maxOutputLines, String name) {
        this.name = name;
        this.linesContainer = new CircularFifoQueue<>(maxOutputLines);
        this.handler = new Thread(() -> {
            // create and connect a new PipedOutputStream for the output pipe
            try (InputStream in = new PipedInputStream(out)) {
                // collect output until we reach the end
                captureOutput(in, linesContainer, partialLine);
            } catch (IOException e) {
                // hmmm... not much we can do here except log, and shut down the read end of the
                // pipe so the writing end will fail
                logger.error("Unable to set up output handler for an action script execution: {}", e);
                OutputHandler.this.close();
            }
        });
        handler.start();
    }

    @Override
    public void close() {
        // stop the thread when the output handler is closed
        handler.interrupt();
    }

    /**
     * Collect current retained lines of output and join them into a single String.
     *
     * <p>If there's a partial line, we don't included it unless it's promoted to a retained line
     * due ot the <code>promotePartial</code> parameter</p>
     * @param promotePartial if true, convert a non-empty partial line to a normal retained line first
     *                       (this is normally done after the command has terminated). The promoted
     *                       line will kick out the oldest retained line if the container is full
     * @return the most recent output lines, joined with newlines
     */
    String assembleOutput(final boolean promotePartial) {
        // prevent doing this while the collector is actively collecting
        synchronized(this) {
            if (promotePartial && partialLine.length() > 0) {
                linesContainer.add(partialLine.toString());
            }
            return String.join("\n", linesContainer);
        }
    }

    /**
     * Called to attempt to collect any remaining output before shutting down the output handler.
     * @param otherPipeEnd {@link OutputStream} that is at the write end of the pipe
     */
    void finish(final @Nonnull OutputStream otherPipeEnd) {
        try {
            otherPipeEnd.close();
        } catch (IOException e) {
            logger.warn("Failed to close write-end of {} pipe after SSH command terminated: {}", name, e);
        }
        // now wait for the collector thread to terminate when it reaches EOF
        while (this.handler.isAlive()) {
            try {
                this.handler.join();
            } catch (InterruptedException e) {
                // keep waiting if we're interrupted
            }
        }
    }


    /**
     * Capture output as it comes in, after each read, resolve it into retained lines + partial.
     *
     * @param in {@link InputStream} from which output can be read
     * @param linesContainer FIFO queue that will accumulate terminated lines of output
     * @param partial {@link StringBuffer} to hold a single partial line
     */
    private void captureOutput(final @Nonnull InputStream in,
                               final @Nonnull Queue<String> linesContainer,
                               final @Nonnull StringBuffer partial) {
        try (Reader reader = new InputStreamReader(in)) {
            final char[] readBuffer = new char[1000];
            // This loop will exit when the reader detects EOF, which should happen when the write
            // end of the pipe is closed by the SSH channel (and after any remaining buffered output
            // is processed). The finish() method above is used to ensure this happens even if somehow
            // the channel leaves the pipe open.
            while (true) {
                // wait for another chunk of output
                int len = reader.read(readBuffer);
                if (len == -1) {
                    // EOF - we're done
                    break;
                }
                do {
                    synchronized (this) {
                        // attach new output to our "partial" buffer
                        partial.append(readBuffer, 0, len);
                        // split  it into individual lines. Final arg limit = -1 means any number of
                        // pieces, and final piece contains text following last match, even if
                        // that's empty
                        String[] lines = partial.toString().split("\\r?\\n", -1);
                        int finalPartialIndex = lines.length - 1;
                        // copy all terminated lines
                        linesContainer.addAll(Arrays.asList(lines).subList(0, finalPartialIndex));
                        // our final entry is either a non-terminated line, or empty if the final
                        // line was terminated. Either way it's our new "partial" value.
                        partial.setLength(0);
                        partial.append(lines[finalPartialIndex]);
                    }
                    // keep going until there's nothing else to read
                } while (reader.ready() && (len = reader.read(readBuffer)) > 0);
                // notify observers that there's new stuff
                setChanged();
                notifyObservers();
            }
        } catch (IOException e) {
            // hmmm... make some noise and then just terminate the close our reader and terminate.
            // the writer should detect broken pipe and do something appropriate
            logger.warn("IOException received while processing output from an action script execution: {}", e);
        }
    }
}
