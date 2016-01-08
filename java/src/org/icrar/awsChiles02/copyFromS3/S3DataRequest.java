/**
 *  (c) UWA, The University of Western Australia
 *  M468/35 Stirling Hwy
 *  Perth WA 6009
 *  Australia
 *
 *  Copyright by UWA, 2015-2015
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
package org.icrar.awsChiles02.copyFromS3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;

/**
 * Created by mboulton on 7/12/2015.
 */
public class S3DataRequest {
    private final AmazonS3Client awsS3Client;
    private long startPosition;
    private int length;
    private String bucketName;
    private String keyName;
    private byte[] s3Data;
    private GetObjectRequest objectRequest;
    private boolean requestComplete = false;
    private final Object lock;

    /**
     *
     * @param awsS3Client
     * @param startPostion
     * @param length
     * @param bucketName
     * @param keyName
     * @param lock
     */
    public S3DataRequest(final AmazonS3Client awsS3Client,
                         long startPostion,
                         int length,
                         String bucketName,
                         String keyName,
                         final Object lock) {
        if (length <= 0) {
            throw new IllegalArgumentException("Length cannot be less than 1");
        }
        this.awsS3Client = awsS3Client;
        this.startPosition = startPostion;
        this.length = length;
        this.bucketName = bucketName;
        this.keyName = keyName;
        s3Data = new byte[length];
        this.lock = lock;
        objectRequest = new GetObjectRequest(bucketName, keyName);
        objectRequest.setRange(startPostion,startPostion+length - 1);
    }

    /**
     *
     * @param startPostion
     * @param length
     */
    public void ResetS3DataRequest(long startPostion, int length) {
        if (length <= 0) {
            throw new IllegalArgumentException("Length cannot be less than 1");
        }
        this.startPosition = startPostion;
        this.length = length;
        if (s3Data.length != length) {
            s3Data = new byte[length];
        }
        objectRequest.setRange(startPostion,startPostion+length - 1);
        requestComplete = false;
    }

    /**
     *
     * @return
     */
    public AmazonS3Client getAwsS3Client() {
        return awsS3Client;
    }

    /**
     *
     * @return
     */
    public long getStartPosition() {
        return startPosition;
    }

    /**
     *
     * @return
     */
    public int getLength() {
        return length;
    }

    /**
     *
     * @return
     */
    public byte[] getS3Data() { return s3Data; }

    /**
     *
     * @param s3Data
     */
    public void setS3Data(byte[] s3Data) { this.s3Data = s3Data; }

    /**
     *
     * @return
     */
    public GetObjectRequest getObjectRequest() {
        return objectRequest;
    }

    /**
     *
     * @return
     */
    public boolean isRequestComplete() {
        return requestComplete;
    }

    /**
     *
     * @param requestComplete
     */
    public void setRequestComplete(boolean requestComplete) {
        this.requestComplete = requestComplete;
    }

    /**
     *
     * @return
     */
    public Object getLock() { return lock; }
}