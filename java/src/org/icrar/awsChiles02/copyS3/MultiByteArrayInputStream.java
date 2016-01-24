/**
 * (c) UWA, The University of Western Australia
 * M468/35 Stirling Hwy
 * Perth WA 6009
 * Australia
 * <p/>
 * Copyright by UWA, 2016
 * All rights reserved
 * <p/>
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * <p/>
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston,
 * MA 02111-1307  USA
 */
package org.icrar.awsChiles02.copyS3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Created by mboulton on 18/01/2016.
 *
 * A <code>MultiByteArrayInputStream</code> represents the logical
 * concatenation of many byte[]s into one <code>InputStream</code>.
 */
public class MultiByteArrayInputStream extends InputStream {
  private static final Log LOG = LogFactory.getLog(MultiByteArrayInputStream.class);

  private final ArrayBlockingQueue<byte[]> inputStreams;
  private ByteArrayInputStream ins;
  private boolean lastStreamAdded = false;
  private boolean mpdinsClosed = false;

  public MultiByteArrayInputStream(int maxQueueDepth) {
    inputStreams = new ArrayBlockingQueue<>(maxQueueDepth);// ArrayDeque<byte[]>();
  }

  /**
   *  Continues reading in the next byte[] if an EOF is reached.
   */
  private void nextStream() throws IOException {
    if (ins != null) {
      ins.close();
      ins = null;
    }

    if (!inputStreams.isEmpty()) {
      // This will get and remove the next stream.
      byte[] bytes = null;
      while (bytes == null) {
        try {
          bytes = inputStreams.take();
        }
        catch (InterruptedException e) {
          // ignore
        }
      }
      ins = new ByteArrayInputStream(bytes);
      // We don't test result of ins as never null (unless OutOfMemory)
    }
    else {
      ins = null;
    }
    LOG.debug("There are " + inputStreams.size() + " elements in inpustStream Queue.");
  }

  private void add(byte[] inputBytes) {
    boolean itemAdded = false;
    synchronized (inputStreams) {
      while (!itemAdded) {
        try {
          inputStreams.put(inputBytes);
          itemAdded = true;
        }
        catch (InterruptedException e) {
          // ignore!
        }
      }
      inputStreams.notifyAll();
    }
  }

  /**
   *
   * @param inputBytes of array to add to the <code>InputStream</code>.
   * @throws IOException if <code>MultiByteArrayInputStream</code> cannot be added to or is closed.
   */
  public void addByteArray(byte[] inputBytes) throws IOException {
    if (lastStreamAdded || mpdinsClosed) {
      throw new IOException("Last stream already added or stream closed");
    }
    add(inputBytes);
  }

  /**
   *
   * @param inputBytes of array to add to the <code>InputStream</code>.
   * @throws IOException if <code>MultiByteArrayInputStream</code> cannot be added to or is closed.
   */
  public void addLastInputStream(byte[] inputBytes) throws IOException {
    if (lastStreamAdded || mpdinsClosed) {
      throw new IOException("Last stream already added or stream closed");
    }
    add(inputBytes);
    lastStreamAdded = true;
  }

  @Override
  public int available() throws IOException {
    if (ins == null) {
      return 0; // no way to signal EOF from available()
    }
    return ins.available();
  }

  /**
   * During read if end of one byte array is reached then get next,
   * waiting if necessary for one to be added.
   *
   * @throws IOException if <code>MultiByteArrayInputStream</code>  is closed.
   */
  private void waitForNextByteArray() throws IOException {
    if (mpdinsClosed) {
      throw new IOException("InputStream closed");
    }
    synchronized (inputStreams) {
      while (inputStreams.isEmpty()) {
        try {
          inputStreams.wait();
        }
        catch (InterruptedException e) {
          LOG.error("Exception", e);
        }
      }
    }
    nextStream();
  }

  @Override
  public int read() throws IOException {
    if (ins == null) {
      if (lastStreamAdded || mpdinsClosed) {
        return -1;
      }
      else {
        waitForNextByteArray();
      }
    }
    int c = ins.read();
    if (c == -1) {
      nextStream();
      return read();
    }
    return c;
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    if (ins == null) {
      if (lastStreamAdded || mpdinsClosed) {
        return -1;
      }
      else {
        waitForNextByteArray();
      }
    }
    else if (b == null) {
      throw new NullPointerException();
    }
    else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    else if (len == 0) {
      return 0;
    }

    int n = ins.read(b, off, len);
    if (n <= 0) {
      nextStream();
      return read(b, off, len);
    }
    return n;
  }

  @Override
  public void close() throws IOException {
    do {
      nextStream();
    }
    while (ins != null);
    mpdinsClosed = true;
  }
}
