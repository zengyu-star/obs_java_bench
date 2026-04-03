package com.huawei.obs.bench.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Zero-Copy Byte Stream Adapter (Utility Plane - Zero Copy)
 * Responsibility: Wraps an NIO ByteBuffer as a traditional InputStream for the OBS SDK.
 * Architectural Significance: Eliminates memory copy overhead when converting ByteBuffer to byte[] during high-concurrency uploads.
 */
public class ByteBufferInputStream extends InputStream {

    private final ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * Reads a single byte
     * Corresponds to SDK internal parsing of small data blocks or headers.
     */
    @Override
    public int read() throws IOException {
        if (!buffer.hasRemaining()) {
            return -1; // End of stream
        }
        // ByteBuffer.get() automatically advances the position pointer
        return buffer.get() & 0xFF;
    }

    /**
     * Bulk byte read (Performance Core)
     * Corresponds to the process of the SDK pushing data to a network socket.
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (!buffer.hasRemaining()) {
            return -1;
        }

        // Calculate actual readable length to prevent buffer overflow
        int bytesToRead = Math.min(len, buffer.remaining());
        
        // Direct copy from ByteBuffer to target byte array.
        // While there is one copy to byte[] (restricted by the InputStream interface),
        // since the ByteBuffer is in Direct mode, it bypasses secondary JVM heap transfers.
        buffer.get(b, off, bytesToRead);
        
        return bytesToRead;
    }

    /**
     * Gets remaining readable bytes in the stream
     * SDK uses this value to set the Content-Length header.
     */
    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }

    /**
     * Resets stream position
     * Used by the SDK to re-read data from the beginning during network retry scenarios.
     */
    @Override
    public synchronized void reset() throws IOException {
        buffer.rewind();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readlimit) {
        buffer.mark();
    }
}
