package com.vmturbo.proactivesupport.communications;

import java.io.IOException;
import java.util.Random;
import java.util.zip.CRC32;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import static com.vmturbo.proactivesupport.communications.AbstractChannel.MAX_MSG_LENGTH_RCV;

/**
 * The TestMessageFrame tests the MessageFrame.
 */
public class TestMessageFrame {

    private byte[] generateData(final int size) {
        final byte[] data = new byte[size];
        final Random rnd = new Random();
        rnd.nextBytes(data);
        return data;
    }

    private long generateCRC32(final @Nonnull byte[] data) {
        final CRC32 crc = new CRC32();
        crc.update(data, 0, data.length);
        return crc.getValue();
    }

    private long generateCRC32(final @Nonnull byte[] data, int offs, int len) {
        final CRC32 crc = new CRC32();
        crc.update(data, offs, len);
        return crc.getValue();
    }

    @Test
    public void testData() {
        byte[] data = generateData(32);
        MessageFrame frame = new MessageFrame(data, 0, data.length, MessageFrame.Flag.ID);
        Assert.assertEquals(data, frame.getData());
        Assert.assertTrue(frame.containsFlag(MessageFrame.Flag.ID));
        Assert.assertFalse(frame.containsFlag(MessageFrame.Flag.EOF));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDataTooLong() throws IllegalArgumentException {
        byte[] data = new byte[MAX_MSG_LENGTH_RCV + 100]; // More than 32K
        new MessageFrame(data, 0, data.length, MessageFrame.Flag.ID);
    }

    @Test
    public void testFlags() {
        byte[] data = generateData(33);
        MessageFrame frame =
                new MessageFrame(data, 0, data.length, MessageFrame.Flag.ID, MessageFrame.Flag.EOF);
        Assert.assertTrue(frame.containsFlag(MessageFrame.Flag.ID));
        Assert.assertFalse(frame.containsFlag(MessageFrame.Flag.MESSAGE));
        Assert.assertTrue(frame.containsFlag(MessageFrame.Flag.EOF));

        frame = new MessageFrame(data, 0, data.length, MessageFrame.Flag.EOF);
        Assert.assertFalse(frame.containsFlag(MessageFrame.Flag.ID));
        Assert.assertFalse(frame.containsFlag(MessageFrame.Flag.MESSAGE));
        Assert.assertTrue(frame.containsFlag(MessageFrame.Flag.EOF));

        frame = new MessageFrame(data, 0, data.length, MessageFrame.Flag.MESSAGE);
        Assert.assertFalse(frame.containsFlag(MessageFrame.Flag.ID));
        Assert.assertTrue(frame.containsFlag(MessageFrame.Flag.MESSAGE));
        Assert.assertFalse(frame.containsFlag(MessageFrame.Flag.EOF));
    }

    @Test
    public void testSerialization() throws IOException {
        byte[] data = generateData(33);
        MessageFrame frame =
                new MessageFrame(data, 0, data.length, MessageFrame.Flag.ID, MessageFrame.Flag.EOF);
        byte[] msg = frame.toByteArray(new byte[frame.getByteArrayLength()], 0);
        MessageFrame frame1 = new MessageFrame(msg);
        Assert.assertEquals(frame.containsFlag(MessageFrame.Flag.ID),
                            frame1.containsFlag(MessageFrame.Flag.ID));
        Assert.assertEquals(frame.containsFlag(MessageFrame.Flag.MESSAGE),
                            frame1.containsFlag(MessageFrame.Flag.MESSAGE));
        Assert.assertEquals(frame.containsFlag(MessageFrame.Flag.EOF),
                            frame1.containsFlag(MessageFrame.Flag.EOF));
        Assert.assertArrayEquals(frame.getData(), frame1.getData());
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testBadSerializationLength() {
        byte[] data = generateData(33);
        MessageFrame frame = new MessageFrame(data, 0, data.length, MessageFrame.Flag.ID);
        byte[] msg = frame.toByteArray(new byte[1], 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooBigDataDeserialization() throws IOException {
        byte[] data = generateData(MessageFrame.MAX_MSG_LENGTH * 2);
        MessageFrame frame = new MessageFrame(data);
    }
}
