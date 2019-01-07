package com.vmturbo.notification.api;

import java.time.Clock;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;

/**
 * API server-side Spring configuration.
 */
@Configuration
@EnableWebMvc
public class TestApiServerConfig extends WebMvcConfigurerAdapter {

    // START bean definitions for API backend.

    @Bean
    public NotificationSender notificationSender() {
        return new NotificationSender(notificationsChannel(), Clock.systemUTC());
    }

    @Bean
    public SenderReceiverPair<SystemNotification> notificationsChannel() {
        return new SenderReceiverPair<>();
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        final GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
        msgConverter.setGson(ComponentGsonFactory.createGson());
        converters.add(msgConverter);
    }
}
