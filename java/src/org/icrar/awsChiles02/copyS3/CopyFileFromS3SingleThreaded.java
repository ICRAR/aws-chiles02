/**
 *  Copyright (c) UWA, The University of Western Australia
 *  M468/35 Stirling Hwy
 *  Perth WA 6009
 *  Australia
 *
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
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
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
public class CopyFileFromS3SingleThreaded {
  private static final Log LOG = LogFactory.getLog(CopyFileFromS3SingleThreaded.class);

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
        formatter.printHelp( "CopyFileFromS3", options );
        return;
      }
    }
    catch(ParseException exp) {
      // oops, something went wrong
      LOG.error( "Parsing failed.", exp);

      // automatically generate the help statement
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "copy from", options );
      return;
    }

    ProfileCredentialsProvider credentialsProvider =
        (profileName == null)
        ? new ProfileCredentialsProvider()
        : new ProfileCredentialsProvider(profileName);
    TransferManager transferManager = new TransferManager(credentialsProvider);

    long startTime = System.currentTimeMillis();

    // TransferManager processes all transfers asynchronously, so this call will return immediately.
    Download download = transferManager.download(bucketName, keyName, new File(filePath));

    try {
      // Block and wait for the upload to finish
      download.waitForCompletion();
    }
    catch (AmazonClientException amazonClientException) {
      LOG.error("Unable to download file, download aborted.", amazonClientException);
      amazonClientException.printStackTrace();
    }
    long endTime = System.currentTimeMillis();
    LOG.info("download took " + (endTime - startTime) / 1000 + " seconds");

    // Close everything down
    transferManager.shutdownNow();
  }
}
