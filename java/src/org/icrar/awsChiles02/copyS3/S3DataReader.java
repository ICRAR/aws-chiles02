/**
 *  (c) UWA, The University of Western Australia
 *  M468/35 Stirling Hwy
 *  Perth WA 6009
 *  Australia
 *
 *  Copyright by UWA, 2015-2016
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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.S3Object;
import org.icrar.awsChiles02.copyS3.S3DataRequest;

import java.io.IOException;

/**
 * Created by mboulton on 7/12/2015.
 */
public class S3DataReader implements Runnable {
    private final S3DataRequest request;
    private final Object lock;

    public S3DataReader(S3DataRequest request) {
        this.request = request;
        lock = request.getLock();
    }

    private void getData() {
        int tries = 3;
        S3Object s3Object = null;
        while (s3Object == null && tries > 0) {
            try {
                s3Object = request.getAwsS3Client().getObject(request.getObjectRequest());
            } catch (Exception e) {
                System.err.println("Caught exception at " + request.getStartPosition() + ", retrying count " + tries + ".");
                e.printStackTrace();
            } // org.apache.http.NoHttpResponseException
        }

        if (s3Object == null) {
            System.err.println("Failed connection for " + request.getStartPosition() + ".");
            request.setFailed(true);
            return;
        }
        int bytesRead = 0;
        int currentPosition = 0;
        while (bytesRead != -1) {
            try {
                bytesRead = s3Object.getObjectContent().read(
                        request.getS3Data(), currentPosition, request.getLength() - currentPosition);
                currentPosition += bytesRead;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // We don't need to set the s3Data bytes in the original request as that was done above in the read.
        request.setRequestComplete(true);
        System.out.println("Thread at position " + request.getStartPosition() + ", read " + currentPosition + " about to do sync/notifyAll");
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    @Override
    public void run() {
        getData();
    }
}
