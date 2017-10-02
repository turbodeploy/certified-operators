package com.turbonomic.telemetry;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.LongSerializationPolicy;

/**
 * The {@link TelemetryElasticLoader} implements the loading into the elasticsearch.
 */
public class TelemetryElasticLoader {
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
     * The function that processes the entities.
     */
    private final BiFunction<String, Long, Integer> entitiesProcessor_;

    /**
     * Constructs the loader.
     *
     * @param index The index to be used in the loader.
     * @throws ElasticLoadingException When there is an error uploading to the elastic search.
     */
    public TelemetryElasticLoader(final @Nonnull String index) throws ElasticLoadingException {
        entitiesProcessor_ = (entity, oid) -> {
            try {
                URL url = new URL("http://localhost:9200/" + index + "/service_entity/" + oid);
                HttpURLConnection connection = (HttpURLConnection)url.openConnection();
                connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                connection.setDoInput(true);
                try (DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
                    String json = GSON.toJson(new Struct(entity, oid));
                    out.write(json.getBytes(CHARSET));
                }
                // Check the results, the JDK way.
                int sum = 0;
                try (InputStream in = connection.getInputStream()) {
                    for (int b = in.read(); b >= 0; b = in.read()) {
                        sum = (sum + b) % 128;
                    }
                }
                // Just prevent the optimizer from yanking the entire read loop from the compiled
                // code.
                return sum % 2;
            } catch (IOException e) {
                throw new ElasticLoadingException("Error uploading the entry to the elastic search", e);
            }
        };
    }

    /**
     * Returns the handler.
     *
     * @return The handler.
     */
    public BiFunction<String, Long, Integer> getEntitiesProcessor() {
        return entitiesProcessor_;
    }

    /**
     * The struct to ease the upload.
     */
    private static class Struct {
        /**
         * The entity type.
         */
        String entityType;

        /**
         * The entity OID.
         */
        Long oid;

        /**
         * Constructs the struct.
         *
         * @param entityType The entity type.
         * @param oid        The entity OID.
         */
        Struct(final @Nonnull String entityType, final @Nonnull Long oid) {
            this.entityType = entityType;
            this.oid = oid;
        }
    }
}
