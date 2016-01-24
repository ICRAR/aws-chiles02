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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarInputStream;

/**
 * Created by mboulton on 18/01/2016.
 *
 * <code>Runnable</code> class to take a <code>MultiByteArrayInputStream</code> and
 * extract contents as a tar archive.
 */
public class TarExtractorThread implements Runnable {
  private static final Log LOG = LogFactory.getLog(TarExtractorThread.class);

  private final MultiByteArrayInputStream mbis;
  private final String destinationPath;
  private boolean extractCompleted = false;

  /**
   *
   * @param mbis the <code>MultiByteArrayInputStream</code> to use to read tar data from.
   * @param destinationPath is directory to extract tar contents into.
   */
  public TarExtractorThread(MultiByteArrayInputStream mbis, String destinationPath) {
    this.mbis = mbis;
    this.destinationPath = destinationPath;
  }

  /**
   *
   * @return true if the extraction has completed, false otherwise.
   */
  public boolean isExtractCompleted() {
    return extractCompleted;
  }

  @Override
  public void run() {
    TarInputStream tis = new TarInputStream(mbis);
    TarEntry entry;
    try {
      while ((entry = tis.getNextEntry()) != null) {
        int count;
        byte data[] = new byte[2048];

        String destination = destinationPath + "/" + entry.getName();

        if (entry.isDirectory()) {
          // Create direcotry and skip to next entry.
          File directory = new File(destination);
          if (!directory.mkdir()) {
            LOG.error("Could not create directory " + destination);
          }
        }
        else {
          // Create file and extract data into it.
          FileOutputStream fos = new FileOutputStream(destination);
          BufferedOutputStream bos = new BufferedOutputStream(fos);

          while ((count = tis.read(data)) != -1) {
            bos.write(data, 0, count);
          }

          bos.flush();
          bos.close();
        }
      }
      tis.close();
    }
    catch (IOException e) {
      LOG.error("Caught IOException, cannot continue!");
      e.printStackTrace();
    }
    extractCompleted = true;
  }
}
