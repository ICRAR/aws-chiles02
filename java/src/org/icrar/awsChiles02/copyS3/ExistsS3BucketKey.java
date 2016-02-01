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


import java.util.List;

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
public class ExistsS3BucketKey extends AbstractCopyS3 {
  private static final Log LOG = LogFactory.getLog(ExistsS3BucketKey.class);

  public static void main(String[] args) throws Exception {
    String bucketName;
    String key;

    String profileName = null;
    String accessKeyId = null;
    String secretAccessKey = null;

    Option awsProfile =
        Option.builder("aws_profile")
              .hasArg()
              .argName("profile_name")
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
      if (mainArguments.size() == 2) {
        bucketName = mainArguments.get(0);
        key = mainArguments.get(1);
      }
      else if (mainArguments.size() == 1) {
        bucketName = getBucketName(mainArguments.get(0));
        key = getKeyName(mainArguments.get(0));
      }
      else {
        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ExistsS3BucketKey [OPTIONS] [s3_url output][bucket key output]", options);
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

    // Now we have options setup dependants
    setupAWS(profileName, accessKeyId, secretAccessKey);

    long fileSize = CopyFileFromS3.getObjectSizePlusMetadata(bucketName, key);
    if (fileSize < 0) {
      // error occurred or file does not exist
      System.exit(1);
    }

    LOG.info("Download file size is " + fileSize);
  }
}
