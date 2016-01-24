/**
 * (c) UWA, The University of Western Australia
 * M468/35 Stirling Hwy
 * Perth WA 6009
 * Australia
 * <p/>
 * Copyright by UWA, 2015-2016
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

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;

/**
 *
 */
public class S3DataRequest {
  private final AmazonS3Client awsS3Client;
  private final long startPosition;
  private final int length;
  private final byte[] s3Data;
  private final GetObjectRequest objectRequest;
  private boolean requestComplete = false;
  private boolean failed = false;
  private final int index;
  private final Object lock;

  /**
   *
   * @param awsS3Client to use for download.
   * @param startPostion is position to start download from in the object.
   * @param length of the segment to download from object.
   * @param bucketName to get object from.
   * @param keyName of object in bucket.
   * @param index of pool this request is associated with. Used only for debug output.
   * @param lock to use for notifications.
   */
  public S3DataRequest(
      final AmazonS3Client awsS3Client,
      long startPostion,
      int length,
      String bucketName,
      String keyName,
      int index,
      final Object lock) {
    if (length <= 0) {
      throw new IllegalArgumentException("Length cannot be less than 1");
    }
    this.awsS3Client = awsS3Client;
    this.startPosition = startPostion;
    this.length = length;
    s3Data = new byte[length];
    this.index = index;
    this.lock = lock;
    objectRequest = new GetObjectRequest(bucketName, keyName);
    objectRequest.setRange(startPostion, startPostion + length - 1);
  }

  /**
   *
   * @return the AmazonS3Client for this request.
   */
  public AmazonS3Client getAwsS3Client() {
    return awsS3Client;
  }

  /**
   *
   * @return the start position to use in this request.
   */
  public long getStartPosition() {
    return startPosition;
  }

  /**
   *
   * @return the length of this request.
   */
  public int getLength() {
    return length;
  }

  /**
   *
   * @return the data downloaded as part of this request from S3.
   */
  public byte[] getS3Data() {
    return s3Data;
  }

  /**
   *
   * @return the <code>GetObjectRequest</code> required during S3 SDK calls.
   */
  public GetObjectRequest getObjectRequest() {
    return objectRequest;
  }

  /**
   *
   * @return true is complete and false otherwise.
   */
  public boolean isRequestComplete() {
    return requestComplete;
  }

  /**
   *
   * @param requestComplete state of this request.
   */
  public void setRequestComplete(boolean requestComplete) {
    this.requestComplete = requestComplete;
  }

  /**
   *
   * @return true is state for this request is failed and true otherwise.
   */
  public boolean isFailed() {
    return failed;
  }

  /**
   *
   * @param failed state of this request.
   */
  public void setFailed(boolean failed) {
    this.failed = failed;
  }

  /**
   *
   * @return the index set during cration of this object.
   */
  public int getIndex() {
    return index;
  }

  /**
   *
   * @return the lock to use for notifications on this request.
   */
  public Object getLock() {
    return lock;
  }
}
