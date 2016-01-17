/**
 *  (c) UWA, The University of Western Australia
 *  M468/35 Stirling Hwy
 *  Perth WA 6009
 *  Australia
 *
 *  Copyright by UWA, 2012-2015
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


import java.io.File;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The type Copy file to s 3.
 */
public class CopyFileToS3 extends AbstractCopyS3 {
  private static final Log LOG = LogFactory.getLog(CopyFileToS3.class);

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws Exception the exception
   */
  public static void main(String[] args) throws Exception {
    CopyFileToS3 copy = new CopyFileToS3();
    copy.doCopy(args);
  }

  /**
   * Do the actual copy
   * @param args the command line arguments
   */
  private void doCopy(String[] args) {
    String bucketName;
    String filePath;
    String keyName;
    String profileName = null;
    String accessKeyId = null;
    String secretAccessKey = null;

    Option awsProfile =
        Option.builder("aws_profile")
              .hasArg()
              .desc("the aws profile to use")
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
      if (line.hasOption("aws_access_key_id")) {
        accessKeyId = line.getOptionValue("aws_access_key_id");
      }
      if (line.hasOption("aws_secret_access_key")) {
        secretAccessKey = line.getOptionValue("aws_secret_access_key");
      }

      List<String> mainArguments = line.getArgList();
      if (mainArguments.size() == 3) {
        bucketName = mainArguments.get(0);
        keyName = mainArguments.get(1);
        filePath = mainArguments.get(2);
      }
      else if (mainArguments.size() == 2) {
        bucketName = getBucketName(mainArguments.get(0));
        keyName = getKeyName(mainArguments.get(0));
        filePath = mainArguments.get(1);
      }
      else {
        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "CopyFileToS3", options );
        return;
      }
    }
    catch(ParseException exp) {
      // oops, something went wrong
      LOG.error( "Parsing failed.", exp);

      // automatically generate the help statement
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "CopyFileToS3 [s3://bucket/dest src]", options );
      return;
    }

    // If we have the key id and secret access key use them
    TransferManager transferManager = getTransferManager(profileName, accessKeyId, secretAccessKey);

    long startTime = System.currentTimeMillis();

    // TransferManager processes all transfers asynchronously, so this call will return immediately.
    Upload upload = transferManager.upload(bucketName, keyName, new File(filePath));

    try {
      // Block and wait for the upload to finish
      upload.waitForCompletion();
    }
    catch (AmazonClientException amazonClientException) {
      LOG.error("Unable to upload file, upload aborted.", amazonClientException);
      amazonClientException.printStackTrace();
    }
    catch (InterruptedException e) {
      LOG.error("Interrupted", e);
    }
    long endTime = System.currentTimeMillis();
    LOG.info("Upload took " + (endTime - startTime) / 1000 + " seconds");

    // Close everything down
    transferManager.shutdownNow();
  }
}
