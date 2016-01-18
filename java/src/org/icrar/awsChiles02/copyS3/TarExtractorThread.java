/**
 *  (c) UWA, The University of Western Australia
 *  M468/35 Stirling Hwy
 *  Perth WA 6009
 *  Australia
 *
 *  Copyright by UWA, 2016
 *  All rights reserved
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston,
 *  MA 02111-1307  USA
 */
package org.icrar.awsChiles02.copyS3;

import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarInputStream;

import java.io.*;

/**
 * Created by mboulton on 18/01/2016.
 */
public class TarExtractorThread implements Runnable {
    private final MultiByteArrayInputStream mbis;
    private final String destinationPath;

    public TarExtractorThread (MultiByteArrayInputStream mbis, String destinationPath) {
        this.mbis = mbis;
        this.destinationPath = destinationPath;
    }

    @Override
    public void run() {
        // Create a TarInputStream
        TarInputStream tis = new TarInputStream(mbis);
        TarEntry entry;
        try {
            while ((entry = tis.getNextEntry()) != null) {
                int count;
                byte data[] = new byte[2048];

                String destination = destinationPath + "/" + entry.getName();

                if (entry.isDirectory()) {
                    File directory = new File(destination);
                    directory.mkdir();
                } else {
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
        } catch (IOException e) {
            e.printStackTrace();;
        }
    }
}
