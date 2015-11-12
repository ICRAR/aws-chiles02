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
package org.icrar.awsChiles02.copyToS3;


import java.io.File;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.model.PutObjectRequest;
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
 *
 */
public class CopyFileToS3 {
  private static final Log LOG = LogFactory.getLog(CopyFileToS3.class);

  public static void main(String[] args) throws Exception {
    String bucketName;
    String filePath;
    String keyName;
    String profileName = null;

    Option awsProfile = Option.builder("aws_profile")
                             .hasArg()
                             .desc("the aws profile to use")
                             .build();

    Options options = new Options();
    options.addOption(awsProfile);

    // create the parser
    CommandLineParser parser = new DefaultParser();
    try {
      // parse the command line arguments
      CommandLine line = parser.parse(options, args);

      if (line.hasOption("aws_profile"))
        profileName = line.getOptionValue("aws_profile");

      List<String> mainArguments = line.getArgList();
      if (mainArguments.size() == 3) {
        bucketName = mainArguments.get(0);
        keyName = mainArguments.get(1);
        filePath = mainArguments.get(2);
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
      formatter.printHelp( "ant", options );
      return;
    }

    ProfileCredentialsProvider credentialsProvider =
        (profileName == null)
        ? new ProfileCredentialsProvider()
        : new ProfileCredentialsProvider(profileName);
    TransferManager tm = new TransferManager(credentialsProvider);

    // For more advanced uploads, you can create a request object
    // and supply additional request parameters (ex: progress listeners,
    // canned ACLs, etc.)
    PutObjectRequest request =
        new PutObjectRequest(
            bucketName,
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
            LOG.info(
                "Transferred bytes: " +
                    progressEvent.getBytesTransferred());
          }
        }
    );

    // TransferManager processes all transfers asynchronously,
    // so this call will return immediately.
    long startTime = System.currentTimeMillis();
    Upload upload = tm.upload(request);

    try {
      // You can block and wait for the upload to finish
      upload.waitForCompletion();
    }
    catch (AmazonClientException amazonClientException) {
      LOG.error("Unable to upload file, upload aborted.", amazonClientException);
      amazonClientException.printStackTrace();
    }
    long endTime = System.currentTimeMillis();
    LOG.info("Upload took " + (endTime - startTime) + " seconds");
  }
}
