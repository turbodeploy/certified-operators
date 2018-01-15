package com.vmturbo.components.common.mail;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.mail.EmailAttachment;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.MultiPartEmail;

import com.google.common.collect.Lists;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;


/**
 * MailManager is a utility class for sending emails. The configurations for
 * the SMTP server and some parameters for sending emails (e.g. from address,
 * username and password for authentication, etc.) are taken from the
 * Turbonomic global settings repository.
 */
public class MailManager {

    private final SettingServiceGrpc.SettingServiceBlockingStub settingService;

    public static String DATE_PATTERN = "yyyy-MM-dd";

    /**
     * Constructor for the mail manager.
     *
     * @param groupChannel Channel for calling RPC of the Group component.
     */
    public MailManager(@Nonnull final Channel groupChannel) {
        this.settingService = SettingServiceGrpc.newBlockingStub(groupChannel);
    }

    /**
     * Send an email.
     *
     * @param addresses A list of recipient email addresses
     * @param subject Email subject
     * @param content Email content
     * @throws MailConfigException Missing or invalid SMPT configuration settings exist
     * @throws MailException Error when sending the email
     */
    public void sendMail(@Nonnull List<String> addresses,
                         @Nonnull String subject,
                         @Nonnull String content)
            throws MailConfigException, MailException {
        sendMail(addresses, subject, content, null);
    }

    /**
     * Send an email with attachments.
     *
     * @param addresses A list of recipient email addresses
     * @param subject Email subject
     * @param content Email content
     * @param attachmentPaths A list of paths to attachments
     * @throws MailConfigException Missing or invalid SMPT configuration settings exist
     * @throws MailException Error when sending the email
     */
    public void sendMail(@Nonnull List<String> addresses,
                         @Nonnull String subject,
                         @Nonnull String content,
                         @Nullable List<String> attachmentPaths)
            throws MailConfigException, MailException {

        Objects.requireNonNull(addresses);
        Objects.requireNonNull(subject);
        Objects.requireNonNull(content);

        // Retrive SMTP configurations from the settings service
        // The configurations are not cached between calls of sendMail to make sure the latest
        // setting values are used.
        MailConfiguration config = new MailConfiguration(settingService);

        try {
            // Add attachments
            List<EmailAttachment> toAttach = Lists.newArrayList();
            if (attachmentPaths != null) {
                for (String a : attachmentPaths) {
                    File f = new File(a);
                    if (f.exists() && f.isFile() && f.canRead()) {
                        EmailAttachment attachment = new EmailAttachment();
                        attachment.setPath(a);
                        attachment.setDisposition(EmailAttachment.ATTACHMENT);
                        attachment.setName(new SimpleDateFormat(DATE_PATTERN)
                                .format(new Date()) + "-" + f.getName());
                        toAttach.add(attachment);
                    } else {
                        throw new MailException("Failed to add attachments: " + attachmentPaths);
                    }
                }
            }

            // Prepare email
            MultiPartEmail email = new MultiPartEmail();
            email.setHostName(config.getSmtpServer());
            email.setSmtpPort(config.getSmtpPort());
            email.setFrom(config.getFromAddress());

            // If username is available, set authentication credentials.
            if (config.getUsername() != null && config.getUsername().isEmpty() == false) {
                email.setAuthentication(config.getUsername(), config.getPassword());
            }

            // If settings indicate encrytion type,
            if (config.getEncryption().equals(MailConfiguration.EncryptionType.SSL)) {
                email.setSSLCheckServerIdentity(true);
                email.setSSLOnConnect(true);
            } else if (config.getEncryption().equals(MailConfiguration.EncryptionType.TLS)) {
                email.setSSLCheckServerIdentity(true);
                email.setStartTLSEnabled(true);
                email.setStartTLSRequired(true);
            }

            // Subject, content (body) and recipients
            email.setSubject(subject);
            email.setMsg(content);
            for (String emailAddr : addresses) {
                email.addTo(emailAddr);
            }

            // Add attachments
            for (EmailAttachment attach : toAttach) {
                email.attach(attach);
            }

            email.send();
        } catch (EmailException e) {
            throw new MailException("Failed to send email.", e);
        }
    }
}
