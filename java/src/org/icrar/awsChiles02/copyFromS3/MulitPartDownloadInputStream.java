package org.icrar.awsChiles02.copyFromS3;

/**
 * Created by mboulton on 9/12/2015.
 *
 * A <code>MulitPartDownloadInputStream</code> represents
 * the logical concatenation of the input streams
 * returned by S3 getObject.
 */

import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * NOTE: This likely won't be needed now as highly likely that it will be more reliable to read
 * into a buffer first so that problems with readers in others Threads doesn't cause a timeout on
 * an InputStream from anothe Thread.
 */
public class MulitPartDownloadInputStream extends InputStream {
    private static final class Lock { }
    private final Object lock = new Lock();
    private static final Queue<S3ObjectInputStream> inputStreams = new ArrayDeque<S3ObjectInputStream>();
    private S3ObjectInputStream ins;
    private boolean lastStreamAdded = false;
    private boolean mpdinsClosed = false;

    /**
     *
     * @param
     * @see     java.util.Enumeration
     */
    public MulitPartDownloadInputStream() {

    }

    /**
     *
     * @param s3ObjectInputStream
     */
    public synchronized void addStream(S3ObjectInputStream s3ObjectInputStream) {
        inputStreams.add(s3ObjectInputStream);
    }

    /**
     *  Continues reading in the next stream if an EOF is reached.
     */
    private final void nextStream() throws IOException {
        if (ins != null) {
            ins.close();
        }

        synchronized (inputStreams) {
            if (!inputStreams.isEmpty()) {
                // This will get and remove the next stream.
                ins = inputStreams.poll();
                if (ins == null)
                    throw new NullPointerException();
            } else {
                ins = null;
            }
        }
    }

    /**
     *
     * @param ins
     * @throws IOException
     */
    public synchronized void addInputStream(S3ObjectInputStream ins) throws IOException {
        if (lastStreamAdded || mpdinsClosed) {
            throw new IOException("Last stream already added or stream closed");
        }
        inputStreams.add(ins);
        inputStreams.notifyAll();
    }

    /**
     *
     * @param ins
     * @throws IOException
     */
    public synchronized void addLastInputStream(S3ObjectInputStream ins) throws IOException {
        if (lastStreamAdded || mpdinsClosed) {
            throw new IOException("Last stream already added or stream closed");
        }
        inputStreams.add(ins);
        lastStreamAdded = true;
        inputStreams.notifyAll();
    }

    /**
     *
     */
    @Override
    public int available() throws IOException {
        if(ins == null) {
            return 0; // no way to signal EOF from available()
        }
        return ins.available();
    }

    private void waitForNextInputStream() throws IOException {
        if (mpdinsClosed) {
            throw new IOException("InputStream closed");
        }
        synchronized (inputStreams) {
            while (inputStreams.isEmpty()) {
                try {
                    inputStreams.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        nextStream();
    }

    /**
     *
     */
    @Override
    public int read() throws IOException {
        if (ins == null) {
            if (lastStreamAdded || mpdinsClosed) {
                return -1;
            } else {
                waitForNextInputStream();
            }
        }
        int c = ins.read();
        if (c == -1) {
            nextStream();
            return read();
        }
        return c;
    }

    /**
     *
     */
    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (ins == null) {
            if (lastStreamAdded || mpdinsClosed) {
                return -1;
            } else {
                waitForNextInputStream();
            }
        } else if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int n = ins.read(b, off, len);
        if (n <= 0) {
            nextStream();
            return read(b, off, len);
        }
        return n;
    }

    /**
     *
     */
    @Override
    public void close() throws IOException {
        do {
            nextStream();
        } while (ins != null);
        mpdinsClosed = true;
    }
}