package com.vmturbo.components.common.diagnostics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.Stack;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;

import com.google.common.collect.Lists;

/**
 * Iterates over zip and nested zip entries in an {@link InputStream}.
 * The iterator returns instances of {@link Diags}, which content can
 * then be parsed.
 * For example:
 * <pre>
 * {@code
 * for (Diags diags : new RecursiveZipReader(is)) {
 *     parse(diags.getContent());
 * }
 *</pre>
 * where {@code is} is an {@link InputStream}.
 */
public class RecursiveZipReader implements Iterable<Diags> {
    private InputStream is;

    public RecursiveZipReader(InputStream is) {
        this.is = is;
    }

    @Override
    public Iterator<Diags> iterator() {
        try {
            return new DiagsIterator();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * An iterator over the zip entries in a zip file that
     * returns instances of {@link Diags}.
     */
    private class DiagsIterator implements Iterator<Diags> {
        private final Stack<InputStream> inputStreamStack = new Stack<>();
        private final List<Scanner> scanners = Lists.newArrayList();
        private ZipInputStream zis;
        private ZipEntry ze;
        private IOException exception = null;

        DiagsIterator() throws IOException {
            InputStream is = RecursiveZipReader.this.is;
            push(is);
        }

        private void push(InputStream is) throws IOException {
            inputStreamStack.push(is);
            zis = new ZipInputStream(is);
            ze = zis.getNextEntry();
        }

        private void pop() {
            inputStreamStack.pop();
            if (!inputStreamStack.isEmpty()) {
                InputStream is = inputStreamStack.peek();
                zis = new ZipInputStream(is);
                try {
                    ze = zis.getNextEntry();
                } catch (IOException e) {
                    exception = e;
                }
            }
        }

        private boolean hasNextIsFalse() {
            scanners.stream().forEach(Scanner::close);
            return false;
        }

        @Override
        public boolean hasNext() {
            if (exception != null) {
                return hasNextIsFalse();
            }
            if (ze != null) {
                return true;
            }
            while (ze == null && !inputStreamStack.isEmpty()) {
                pop();
            }
            if (ze == null) {
                return hasNextIsFalse();
            }
            return true;
        }

        @Override
        public Diags next() {
            try {
                if (ze == null) {
                    throw new NoSuchElementException();
                }
                String name = ze.getName();
                if (name.endsWith(".zip")) {
                    push(zis);
                    return next();
                } else if (name.endsWith(".diags") || name.endsWith(".txt")) { // TODO: add to constructor?
                    Diags diags = new Diags(ze.getName(), read(zis));
                    ze = zis.getNextEntry();
                    return diags;
                } else if (name.endsWith(".binary")) {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    IOUtils.copy(zis, bos);
                    Diags diags = new Diags(ze.getName(), bos.toByteArray());
                    ze = zis.getNextEntry();
                    return diags;
                } else {
                    ze = zis.getNextEntry();
                    return next();
                }
            } catch (IOException ioe) {
                NoSuchElementException nsee = new NoSuchElementException();
                nsee.initCause(ioe);
                throw nsee;
            }
        }

        /**
         * Read the lines of the zip entry and return them as a list.
         * @param zis zip input stream containing the lines
         * @return the lines as a list of strings
         */
        private List<String> read(ZipInputStream zis) {
            Scanner sc = new Scanner(zis);
            List<String> lines = Lists.newArrayList();
            while (sc.hasNext()) {
                String line = sc.nextLine();
                lines.add(line);
            }
            // Can't close the scanner here because it will mark the whole input stream as closed
            scanners.add(sc);
            return lines;
        }
    }
}
