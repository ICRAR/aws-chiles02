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


import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.model.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpStatus;

/**
 *
 */
public class CopyFileFromS3 extends AbstractCopyS3 {
  private static final Log LOG = LogFactory.getLog(CopyFileFromS3.class);

  private static final int DEFAULT_THREAD_POOL_SIZE = 16;
  private static final int DEFAULT_REQUEST_LENGTH = 128 * 1024 * 1024; // 128 MBytes

  private static ExecutorService myExecutor = null;
  private static S3DataRequest[] s3DataRequests = null;
  private static ObjectMetadata objectMetadata;

  private static long requestedFileCurrentPosition = 0;

  // This lock is used to sync between the Threads so we know when they are ready.
  private static final class Lock {
  }

  private final Object lock = new Lock();

  private final long fileSize;
  private final int requestLength;
  private final int threadPoolSize;
  private final String bucketName;
  private final String key;
  private final String destinationPath;
  private final boolean extractTar;
  private final MessageDigest digest;

  /**
   *
   * @param fileSize of object to download.
   * @param requestLength of each request, effectively the buffer size for a read.
   * @param threadPoolSize used for number of simulataneous downloads.
   * @param bucketName to get object from.
   * @param key name of object to download.
   * @param destinationPath to send object to or directory to extract contents of object to.
   * @param extractTar treat object to download as tar archive and extract to <code>destinationPath</code>
   * @throws NoSuchAlgorithmException
   */
  private CopyFileFromS3(
      long fileSize,
      int requestLength,
      int threadPoolSize,
      String bucketName,
      String key,
      String destinationPath,
      boolean extractTar)
  throws NoSuchAlgorithmException {
    this.fileSize = fileSize;
    this.requestLength = requestLength;
    this.threadPoolSize = threadPoolSize;
    this.bucketName = bucketName;
    this.key = key;
    this.destinationPath = destinationPath;
    this.extractTar = extractTar;
    this.digest = MessageDigest.getInstance("MD5");
  }

  /**
   * Do the actual download of the S3 file.
   */
  private String go() {
    long currentFilePosition = 0;
    int index = 0;

    initBufferRequest();

    File outFile = new File(destinationPath);
    if (extractTar && !outFile.isDirectory()) {
      LOG.error("Tar extraction requested but destination either doesn't exist or is not a directory.");
      return null;
    }
    else if (!extractTar && outFile.exists()) {
      LOG.debug("Output file " + outFile.getAbsolutePath() + " exists, overwritting!");
      if (!outFile.delete()) {
        LOG.error("Unable to delete file destinationPath!");
        return null;
      }
    }

    if (!extractTar) {
      try {
        if (!outFile.createNewFile()) {
          LOG.error("unable to create file " + destinationPath);
          return null;
        }
      }
      catch (IOException e) {
        LOG.error("Exception", e);
        return null;
      }
    }

    FileOutputStream fos = null;
    MultiByteArrayInputStream mbai = null;
    TarExtractorThread tet = null;
    if (extractTar) {
      mbai = new MultiByteArrayInputStream(threadPoolSize / 2);
      tet = new TarExtractorThread(mbai, destinationPath);
      Thread tetThread = new Thread(tet);
      tetThread.start();
    }
    else {
      try {
        fos = new FileOutputStream(outFile);
      }
      catch (FileNotFoundException e) {
        LOG.error("Exception", e);
        return null;
      }
    }

    BufferedOutputStream bos = null;
    if (!extractTar) {
      bos = new BufferedOutputStream(fos);
    }

    while (currentFilePosition < fileSize) {
      synchronized (lock) {
        // Loop here until the next segmen of the file is ready. We can't wait on a particular thread
        // hence the reason to check each time we get control back from lock.wait().
        while (!s3DataRequests[index].isRequestComplete() && !s3DataRequests[index].isFailed()) {
          try {
            LOG.debug("Waiting for request index " + index);
            lock.wait(30000);
          }
          catch (InterruptedException e) {
            LOG.warn("Exception", e);
          }
        }
      }

      if (s3DataRequests[index].isFailed()) {
        LOG.error("Got FAILED for index " + index + " at position " + s3DataRequests[index].getStartPosition()
                      + " and length " + s3DataRequests[index].getS3Data().length
                      + ". currentFilePosition is " + currentFilePosition);
        return null;
      }

      if (extractTar) {
        try {
          // As we create a new request we know the bytes won't get overwritten.
          // Also this and digest update don't change bytes so n problem them using the same object.
          mbai.addByteArray(s3DataRequests[index].getS3Data());
        }
        catch (IOException e) {
          LOG.error("Failed adding read byte array to MultiByteArrayInputStream", e);
          return null;
        }
      }
      else {
        try {
          bos.write(s3DataRequests[index].getS3Data());
        }
        catch (IOException e) {
          LOG.error("Failed on write to file", e);
          return null;
        }
      }

      digest.update(s3DataRequests[index].getS3Data());

      LOG.debug(
          "Got complete for index " + index + " at position " + s3DataRequests[index].getStartPosition()
              + " and length " + s3DataRequests[index].getS3Data().length
              + ". currentFilePosition is " + currentFilePosition);
      currentFilePosition += s3DataRequests[index].getS3Data().length;
      LOG.info("Download " + (currentFilePosition + 1) * 100L / fileSize + "%");

      doNextRequest(index);
      index++;
      if (index >= s3DataRequests.length) {
        index = 0;
      }
    }

    if (extractTar) {
      // We need to wait for the tar extraction to complete otherwise we might prematurely stop the extractor.
      while (!tet.isExtractCompleted()) {
        try {
          // Busy wait but is it worth lock and notify code given extraction should keep up????
          Thread.sleep(100);
        }
        catch (InterruptedException e) {
          // ignore
        }
      }
    }
    else {
      // Flush and close associate streams.
      try {
        bos.flush();
        bos.close();
        fos.close();
      }
      catch (IOException e) {
        LOG.error("Exception", e);
      }
    }

    byte[] bytes = digest.digest();
    return GetMD5.digestDecimalToHex(bytes);
  }

  /**
   * Request the next segment of the file associating it with position <code>index</code>
   * in <code>s3DataRequests</code>.
   *
   * @param index to do request on.
   */
  private void doNextRequest(int index) {
    if (requestedFileCurrentPosition < fileSize) {
      int requestSize = requestedFileCurrentPosition + requestLength < fileSize ?
                        requestLength : (int) (fileSize - requestedFileCurrentPosition);
      s3DataRequests[index] = new S3DataRequest(
          amazonS3Client,
          requestedFileCurrentPosition,
          requestSize,
          bucketName,
          key,
          index,
          lock);
      myExecutor.execute(new S3DataReader(s3DataRequests[index]));
      requestedFileCurrentPosition += requestSize;
    }
  }

  /**
   *
   * @return the index last used.
   */
  private int initBufferRequest() {
    int index;

    for (index = 0; index < threadPoolSize; index++) {
      if (requestedFileCurrentPosition + requestLength > fileSize) {
        break;
      }
      s3DataRequests[index] = new S3DataRequest(
          amazonS3Client,
          requestedFileCurrentPosition,
          requestLength,
          bucketName,
          key,
          index,
          lock);
      requestedFileCurrentPosition += requestLength;
      try {
        Thread.sleep(100);
      }
      catch (InterruptedException e) {
        // We don't care if we are interrupted!
      }
    }
    if (requestedFileCurrentPosition < fileSize && index != threadPoolSize) {
      s3DataRequests[index++] = new S3DataRequest(
          amazonS3Client,
          requestedFileCurrentPosition,
          (int) (fileSize - requestedFileCurrentPosition),
          bucketName,
          key,
          index,
          lock);
    }

    for (int i = 0; i < index; i++) {
      myExecutor.execute(new S3DataReader(s3DataRequests[i]));
    }

    return index;
  }

    /*
    Test routine.
     */
    /*
    private void test1() {
        String bucketName = "a-c-test";
        String filePath = "t1";
        String keyName = "t1";
        File file = new File(filePath);
        MultiPartDownloadInputStream mpdis = new MultiPartDownloadInputStream();

        GetObjectRequest[] objectRequests = new GetObjectRequest[1];
        objectRequests[0] = new GetObjectRequest(bucketName, keyName);
        //objectRequests[1] = new GetObjectRequest(bucketName, keyName);
        objectRequests[0].setRange(0,5);
        //objectRequests[1].setRange(6,11);
        S3Object[] s3Objects = new S3Object[1];
        s3Objects[0] = awsS3Client.getObject(objectRequests[0]);
        //s3Objects[1] = awsS3Client.getObject(objectRequests[1]);
        byte[] b = new byte[12];
        try {
            s3Objects[0].getObjectContent().read(b,0,6);
            //s3Objects[1].getObjectContent().read(b,6,6);
        } catch (IOException e) {
            e.printStackTrace();
        }

        objectRequests[0].setRange(6,11);
        s3Objects[0] = awsS3Client.getObject(objectRequests[0]);
        try {
            s3Objects[0].getObjectContent().read(b,6,6);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.print(":");
        for (int i = 0; i < b.length; i++) {
            System.out.print(b[i] + ",");
        }
        System.out.println(":");
    }*/

  /**
   *
   * @param bucketName to find key in.
   * @param key of object to get metada for.
   * @return the size of the object referred to by key and bucketName, -1 if key doesn't exist.
   */
  private static long getObjectSizePlusMetadata(String bucketName, String key) {
    GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(bucketName, key);
    try {
      objectMetadata = amazonS3Client.getObjectMetadata(getObjectMetadataRequest);
    }
    catch (AmazonS3Exception e) {
      // Not good to use exception for flow control AWS does not have a "fileExists" type method.
      if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        LOG.debug("Could not find metadata for " + key);
        return -1;
      }
      else {
        throw e;
      }
    }
    return objectMetadata.getContentLength();
  }

  /**
   * Given the bucketName and originaloFileKey of a downloaded object determine, if it exists,
   * the original MD5 hash based on the contents of a separate originaloFileKey.md5 file.
   *
   * @param bucketName to look for the md5 file.
   * @param originalFileKey of the file downloaded.
   * @return null if no md5 hash found and the md5 hash otherwise.
   */
  private static String getMD5FromSeparateMD5File(String bucketName, String originalFileKey) {
    String key = originalFileKey + ".md5";

    GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(bucketName, key);
    ObjectMetadata objMetadata ;
    try {
      objMetadata = amazonS3Client.getObjectMetadata(getObjectMetadataRequest);
    }
    catch (AmazonS3Exception e) {
      LOG.debug("Could not find metadata for " + key, e);
      return null;
    }

    // If storage class is null then assume in STANDARD so continue!
    if (objMetadata.getStorageClass() != null && objMetadata.getStorageClass().equals(StorageClass.Glacier.toString())) {
      LOG.warn("MD5 file is in glacier, cannot get the MD5 value.");
      return null;
    }

    GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
    S3Object s3Object = null;
    try {
      s3Object = amazonS3Client.getObject(getObjectRequest);
    }
    catch (AmazonS3Exception e) {
      // Not good to use exception for flow control AWS does not have a "fileExists" type method.
      if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        LOG.debug("Could not find a .md5 file for file " + originalFileKey);
      }
      else {
        LOG.error("Exception", e);
        return null;
      }
    }

    if (s3Object != null) {
      byte[] bytes = new byte[32];
      int retValue = 0;
      try {
        retValue = s3Object.getObjectContent().read(bytes);
      }
      catch (IOException e) {
        LOG.error("Exception", e);
      }
      if (retValue == 32) {
        LOG.debug("Getting original file hash from S3 file " + key);
        return new String(bytes);
      }
    }
    return null;
  }

  /**
   * Helper method to get the MD5 checksum.
   *
   * NOTE: uses ETAG unless it contains a '-' as that means it is incorrect due to multipart upload.
   * Will also look for
   * s3cmd-attrs - search for md5:######
   * md5 file - search for a file of the form key.md5 in the bucketName and use value stroed in it as the hash
   *
   * @param bucketName to find key in.
   * @param key of object to get metada for.
   * @return MD5 hash if a valid one can be found or null otherwise.
   */
  private static String getMD5(String bucketName, String key) {
    String md5 = objectMetadata.getETag();
    if (md5.contains("-")) {
      LOG.debug("ETag contained a '-' so was generated from multipart upload, looking for alternative MD5");
      // Clear this in case we can't find a valid value
      md5 = null;
      // Case 1, look for s3cmd-attrs md5 param
      String s3cmdAttrs = objectMetadata.getUserMetaDataOf("s3cmd-attrs");
      if (s3cmdAttrs != null) {
        for (String attr : s3cmdAttrs.split("/")) {
          String[] keyVal = attr.split(":");
          if (keyVal.length != 2) {
            continue;
          }
          if (keyVal[0].matches("md5")) {
            LOG.debug("Using s3cmd-attrs user metadata md5 hash");
            md5 = keyVal[1];
          }
        }
      }
      else {
        md5 = getMD5FromSeparateMD5File(bucketName, key);
      }
    }
    if (md5 != null && md5.length() != 32) {
      LOG.error("MD5 checksum was not 32 characters long!");
      md5 = null;
    }
    return md5;
  }

  public static void main(String[] args) throws Exception {
    String bucketName;
    String key;
    String destinationPath;
    boolean extractTar = false;
    int threadBufferSize = DEFAULT_REQUEST_LENGTH;
    int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;

    String profileName = null;
    String accessKeyId = null;
    String secretAccessKey = null;

    Option awsProfile =
        Option.builder("aws_profile")
            .hasArg()
            .argName("profile_name")
            .desc("the aws profile to use")
            .build();

    Option threadBuffer =
        Option.builder("thread_buffer")
            .hasArg()
            .argName("SIZE")
            .desc("number of bytes in each thread's buffer, size of each download part")
            .build();

    Option threadPoolSizeOption =
        Option.builder("thread_pool")
            .hasArg()
            .argName("SIZE")
            .desc("number of simultaneous download threads")
            .build();

    Option extractTarOption =
        Option.builder("extract_tar")
            .hasArg(false)
            .desc("treat file as tar archive and extract to <Destination Path> directory")
            .build();
    Option awsAccessKeyId =
        Option.builder("aws_access_key_id")
            .hasArg()
            .desc("the aws_access_key_id to use")
            .build();
    Option awsSecretAccessKey =
        Option.builder("aws_secret_access_key")
            .hasArg()
            .desc("the aws_secret_access_key to use")
            .build();

    Options options = new Options();
    options.addOption(awsProfile);
    options.addOption(threadBuffer);
    options.addOption(threadPoolSizeOption);
    options.addOption(extractTarOption);
    options.addOption(awsAccessKeyId);
    options.addOption(awsSecretAccessKey);

    // create the parser
    CommandLineParser parser = new DefaultParser();
    try {
      // parse the command line arguments
      CommandLine line = parser.parse(options, args);

      if (line.hasOption("aws_profile")) {
        profileName = line.getOptionValue("aws_profile");
      }

      if (line.hasOption("thread_buffer")) {
        String value = line.getOptionValue("thread_buffer");
        LOG.debug("Found thread_buffer option with value " + value);
        threadBufferSize = Integer.parseInt(value);
        LOG.debug("Option is integer with value " + threadBufferSize);
      }

      if (line.hasOption("thread_pool")) {
        String value = line.getOptionValue("thread_pool");
        LOG.debug("Found thread_pool option with value " + value);
        threadPoolSize = Integer.parseInt(value);
        LOG.debug("Option is integer with value " + threadBufferSize);
      }

      if (line.hasOption("extract_tar")) {
        extractTar = true;
      }
      if (line.hasOption("aws_access_key_id")) {
        accessKeyId = line.getOptionValue("aws_access_key_id");
      }
      if (line.hasOption("aws_secret_access_key")) {
        secretAccessKey = line.getOptionValue("aws_secret_access_key");
      }

      List<String> mainArguments = line.getArgList();
      if (mainArguments.size() == 3) {
        bucketName = mainArguments.get(0);
        key = mainArguments.get(1);
        destinationPath = mainArguments.get(2);
      }
      else if (mainArguments.size() == 2) {
        bucketName = getBucketName(mainArguments.get(0));
        key = getKeyName(mainArguments.get(0));
        destinationPath = mainArguments.get(1);
      }
      else {
        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("CopyFileFromS3 [OPTIONS] [s3_url output][bucket key output]", options);
        return;
      }
    }
    catch (ParseException exp) {
      // oops, something went wrong
      LOG.error("Parsing failed.", exp);

      // automatically generate the help statement
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("CopyFileFromS3", options);
      return;
    }

    boolean downloadFailed = false;

    // Now we have options setup dependants
    setupAWS(profileName, accessKeyId, secretAccessKey);

    myExecutor = Executors.newFixedThreadPool(threadPoolSize);
    try {
      // From here on we always want to shut down the threads so we can exit gracefully, ie not hang!
      s3DataRequests = new S3DataRequest[threadPoolSize];

      // Start timer here so we get time of file transfer without setup time being added
      long startTime = System.currentTimeMillis();

      long fileSize = getObjectSizePlusMetadata(bucketName, key);
      if (fileSize < 0) {
        // error occurred or file does not exist
        return;
      }

      LOG.info("Download file size is " + fileSize);
      CopyFileFromS3 me = new CopyFileFromS3(
              fileSize,
              threadBufferSize,
              threadPoolSize,
              bucketName,
              key,
              destinationPath,
              extractTar);
      String downloadChecksum = me.go();
      long finishTime = System.currentTimeMillis();
      if (downloadChecksum == null) {
        LOG.error("Download failed, exiting.");
        downloadFailed = true;
      } else {
        String md5 = getMD5(bucketName, key);
        LOG.info("Checksums " + (downloadChecksum.equals(md5) ? "matches" : "does not match")
                + " MD5 hash is " + downloadChecksum + " and original file was " + md5);
        LOG.debug("At end of main, about to shutdown Executor");
      }
      LOG.debug("Finished in " + (finishTime - startTime) + ".");
    } finally {
      // Shut down the threads
      myExecutor.shutdown();
      if (!myExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.warn("Forcing Executor shutdown");
        myExecutor.shutdownNow();
      }
      if (downloadFailed) {
        System.exit(1);
      }
    }
  }
}
