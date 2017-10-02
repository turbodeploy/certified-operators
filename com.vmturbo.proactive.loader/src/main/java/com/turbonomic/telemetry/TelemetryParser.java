package com.turbonomic.telemetry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.LongSerializationPolicy;

/**
 * The {@link TelemetryParser} implements parsing of the telemetry topology.
 */
public class TelemetryParser {
    /**
     * The JSON builder.
     */
    private static final Gson GSON = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .setLongSerializationPolicy(LongSerializationPolicy.STRING)
            .setPrettyPrinting()
            .create();

    /**
     * The serialization character set.
     */
    private static final Charset CHARSET = Charset.forName("UTF-8");

    /**
     * Parse the entities.
     *
     * @param in      The input stream.
     * @param handler The handler.
     * @throws IOException In case of an error reading the data.
     */
    @SuppressWarnings("unchecked")
    public void parseEntities(final @Nonnull InputStream in,
                              final @Nonnull BiFunction<String, Long, Integer> handler)
            throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, CHARSET));
        Map<String, String> entity;
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            Map<String, Object> map = GSON.fromJson(line, Map.class);
            entity = (Map<String, String>)map.get("entity");
            handler.apply(entity.get("entityType"), Long.parseLong(map.get("oid").toString()));
        }
    }
}
