package com.vmturbo.topology.processor.rest;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.google.gson.Gson;

/**
 * Utility class to call REST methods in tests.
 */
public class RestCaller {
    private final Gson gson = new Gson();

    private final MockMvc mockMvc;

    public RestCaller(@Nonnull final MockMvc mockMvc) {
        this.mockMvc = Objects.requireNonNull(mockMvc);
    }

    @Nonnull
    public String postAndExpect(
            @Nonnull final String path,
             @Nonnull final HttpStatus expectStatus) throws Exception {
        final MvcResult result = mockMvc.perform(post(path)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().is(expectStatus.value()))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andReturn();
        return result.getResponse().getContentAsString();
    }

    @Nonnull
    public <T> T postAndExpect(
            @Nonnull final String path,
            @Nonnull final Object content,
            @Nonnull final HttpStatus expectStatus,
            Class<T> retClass) throws Exception {
        final String body = gson.toJson(content);
        final MvcResult result = mockMvc.perform(post(path)
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .content(body)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().is(expectStatus.value()))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andReturn();
        return gson.fromJson(result.getResponse().getContentAsString(), retClass);
    }


    @Nonnull
    public <T> T getResult(@Nonnull final String path, Class<T> retClass) throws Exception {
        final MvcResult result = mockMvc.perform(get(path)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andReturn();
        return gson.fromJson(result.getResponse().getContentAsString(), retClass);
    }

}
