package org.icrar.awsChiles02.copyToS3;


import java.io.File;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

/**
 *
 */
public class CopyToS3 {
  public static void main(String[] args) throws Exception {
    String existingBucketName = "*** Provide bucket name ***";
    String keyName = "*** Provide object key ***";
    String filePath = "*** file to upload ***";

    TransferManager tm = new TransferManager(new ProfileCredentialsProvider());

    // For more advanced uploads, you can create a request object
    // and supply additional request parameters (ex: progress listeners,
    // canned ACLs, etc.)
    PutObjectRequest request =
        new PutObjectRequest(
            existingBucketName,
            keyName,
            new File(filePath)
        );

    // You can ask the upload for its progress, or you can
    // add a ProgressListener to your request to receive notifications
    // when bytes are transferred.
    request.setGeneralProgressListener(
        new ProgressListener() {
          @Override
          public void progressChanged(ProgressEvent progressEvent) {
            System.out.println(
                "Transferred bytes: " +
                    progressEvent.getBytesTransferred());
          }
        }
    );

    // TransferManager processes all transfers asynchronously,
    // so this call will return immediately.
    Upload upload = tm.upload(request);

    try {
      // You can block and wait for the upload to finish
      upload.waitForCompletion();
    }
    catch (AmazonClientException amazonClientException) {
      System.out.println("Unable to upload file, upload aborted.");
      amazonClientException.printStackTrace();
    }
  }
}
