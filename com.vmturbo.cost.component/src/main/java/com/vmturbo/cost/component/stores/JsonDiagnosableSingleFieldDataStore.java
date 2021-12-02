package com.vmturbo.cost.component.stores;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.gson.Gson;

import org.apache.commons.io.output.CloseShieldOutputStream;

import com.vmturbo.components.common.diagnostics.DiagnosticsException;

/**
 * Single field data store diagnosable in JSON format.
 *
 * @param <T> type of data.
 */
public class JsonDiagnosableSingleFieldDataStore<T> implements DiagnosableSingleFieldDataStore<T> {

    private final SingleFieldDataStore<T> singleFieldDataStore;
    private final String diagnosticFileName;
    private final Gson gson;
    private final Class<T> typeOfT;
    private final Charset charset;

    /**
     * Constructor.
     *
     * @param singleFieldDataStore the {@link SingleFieldDataStore}.
     * @param diagnosticFileName the file name which will hold diagnostics.
     * @param gson the {@link Gson}.
     * @param typeOfT the class of generic type parameter.
     * @param charset the {@link Charset} of diagnostic.
     */
    public JsonDiagnosableSingleFieldDataStore(
            @Nonnull final SingleFieldDataStore<T> singleFieldDataStore,
            @Nonnull final String diagnosticFileName, @Nonnull final Gson gson,
            @Nonnull final Class<T> typeOfT, @Nonnull final Charset charset) {
        this.singleFieldDataStore = singleFieldDataStore;
        this.diagnosticFileName = diagnosticFileName;
        this.gson = gson;
        this.typeOfT = typeOfT;
        this.charset = charset;
    }

    /**
     * Constructor.
     *
     * @param singleFieldDataStore the {@link SingleFieldDataStore}.
     * @param diagnosticFileName the file name which will hold diagnostics.
     * @param gson the {@link Gson}.
     * @param typeOfT the class of generic type parameter.
     */
    public JsonDiagnosableSingleFieldDataStore(
            @Nonnull final SingleFieldDataStore<T> singleFieldDataStore,
            @Nonnull final String diagnosticFileName, @Nonnull final Gson gson,
            @Nonnull final Class<T> typeOfT) {
        this(singleFieldDataStore, diagnosticFileName, gson, typeOfT, StandardCharsets.UTF_8);
    }

    @Nonnull
    @Override
    public String getFileName() {
        return diagnosticFileName;
    }

    @Override
    public void collectDiags(@Nonnull final OutputStream appender) throws IOException {
        try (OutputStreamWriter writer = new OutputStreamWriter(
                new CloseShieldOutputStream(appender), this.charset)) {
            this.gson.toJson(getData().orElse(null), this.typeOfT, writer);
            writer.flush();
        }
    }

    @Override
    public void restoreDiags(@Nonnull final byte[] bytes, @Nullable final Void context)
            throws DiagnosticsException {
        try (InputStream in = new ByteArrayInputStream(bytes);
             Reader reader = new InputStreamReader(in, this.charset)) {
            setData(this.gson.fromJson(reader, this.typeOfT));
        } catch (IOException e) {
            throw new DiagnosticsException(e);
        }
    }

    @Override
    public void setData(@Nullable final T data) {
        this.singleFieldDataStore.setData(data);
    }

    @Nonnull
    @Override
    public Optional<T> getData() {
        return this.singleFieldDataStore.getData();
    }
}
