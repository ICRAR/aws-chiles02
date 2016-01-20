package org.icrar.awsChiles02.copyS3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;

/**
 * Common code for the copying
 */
abstract class AbstractCopyS3 {
  /**
   * Gets bucket name.
   *
   * @param s3String the s 3 string
   * @return the bucket name
   */
  protected static String getBucketName(String s3String) {
    if (s3String.startsWith("s3://")) {
      int index = s3String.indexOf('/', 5);
      return s3String.substring(5, index);
    }
    return null;
  }

  /**
   * Gets key name.
   *
   * @param s3String the s 3 string
   * @return the key name
   */
  protected static String getKeyName(String s3String) {
    if (s3String.startsWith("s3://")) {
      int index = s3String.indexOf('/', 5);
      return s3String.substring(index + 1);
    }
    return null;
  }

  /**
   * Gets transfer manager.
   *
   * @param profileName     the profile name
   * @param accessKeyId     the access key id
   * @param secretAccessKey the secret access key
   * @return the transfer manager
   */
  protected TransferManager getTransferManager(String profileName, String accessKeyId, String secretAccessKey) {
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.setConnectionTimeout(6 * 60 * 60 * 1000);
    clientConfiguration.setProtocol(Protocol.HTTP);

    AmazonS3Client amazonS3Client;
    if (accessKeyId != null && secretAccessKey != null) {
      BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
      amazonS3Client = new AmazonS3Client(awsCredentials, clientConfiguration);
    }
    else {
      ProfileCredentialsProvider credentialsProvider =
          (profileName == null)
          ? new ProfileCredentialsProvider()
          : new ProfileCredentialsProvider(profileName);
      amazonS3Client = new AmazonS3Client(credentialsProvider, clientConfiguration);
    }
    return new TransferManager(amazonS3Client);
  }
}
