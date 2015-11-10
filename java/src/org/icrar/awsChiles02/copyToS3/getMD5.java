/**
 * (c) UWA, The University of Western Australia
 * M468/35 Stirling Hwy
 * Perth WA 6009
 * Australia
 * <p/>
 * Copyright by UWA, 2012-2015
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
package org.icrar.awsChiles02.copyToS3;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 */
public class GetMD5 {
  private static final Log LOG = LogFactory.getLog(CopyFileToS3.class);

  public static void main(String[] args) throws Exception {
    if (args.length == 1) {
      File file = new File(args[0]);
      String md5 = getFileChecksum(MessageDigest.getInstance("MD5"), file);
      LOG.info("MD5: " + md5);
    }
  }

  /**
   * Calculate the digest
   * @param digest the type of message Digest to use
   * @param file the file to check
   * @return the MD5 as a hex string
   * @throws IOException
   */
  private static String getFileChecksum(MessageDigest digest, File file) throws IOException {
    // Get file input stream for reading the file content
    FileInputStream fis = new FileInputStream(file);

    //Create byte array to read data in chunks
    byte[] byteArray = new byte[65536];
    int bytesCount;

    // Read file data and update in message digest
    while ((bytesCount = fis.read(byteArray)) != -1) {
      digest.update(byteArray, 0, bytesCount);
    }

    // close the stream; We don't need it now.
    fis.close();

    // Get the hash's bytes
    byte[] bytes = digest.digest();

    // This bytes[] has bytes in decimal format;
    // Convert it to hexadecimal format
    StringBuilder sb = new StringBuilder();
    for (byte aByte : bytes) {
      sb.append(Integer.toString((aByte & 0xff) + 0x100, 16).substring(1));
    }

    //return complete hash
    return sb.toString();
  }
}
