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


import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
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
public class CopyFileFromS3 {
    private static final Log LOG = LogFactory.getLog(CopyFileFromS3.class);

    private static final int THREAD_POOL_SIZE = 20;
    private static final int DEFAULT_REQUEST_LENGTH = 10 * 1024 * 1024; // 10MBytes
    private static final ExecutorService MY_EXECUTOR = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    private static final S3DataRequest[] S3_DATA_REQUESTS = new S3DataRequest[THREAD_POOL_SIZE];

    private static ProfileCredentialsProvider credentialsProvider;
    private static AmazonS3Client awsS3Client;

    private static ObjectMetadata objectMetadata;

    private static long requestedFileCurrentPosition = 0;

    // This lock is used to sync between the Threads so we know when they are ready.
    private static final class Lock { }
    private final Object lock = new Lock();

    private final long fileSize;
    private final int requestLength;
    private final String bucketName;
    private final String key;
    private final String destinationPath;
    private final MessageDigest digest;

    private CopyFileFromS3(long fileSize, int requestLength, String bucketName, String key, String destinationPath)
            throws NoSuchAlgorithmException {
        this.fileSize = fileSize;
        this.requestLength = requestLength;
        this.bucketName = bucketName;
        this.key = key;
        this.destinationPath = destinationPath;
        this.digest = MessageDigest.getInstance("MD5");
    }

    /**
     * Do the actual download of the S3 file.
     */
    private String go() {
        int bufferElementsLoaded = initBufferRequest();
        long currentFilePosition = 0;
        int index = 0;

        File outFile = new File(destinationPath);
        if (outFile.exists()) {
            LOG.info("Output file " + outFile.getAbsolutePath() + " exists, overwritting!");
            outFile.delete();
        }

        try {
            outFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(outFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedOutputStream bos = new BufferedOutputStream(fos);

        while (currentFilePosition < fileSize) {
            synchronized (lock) {
                // Loop here until the next segmen of the file is ready. We can't wait on a particular thread
                // hence the reason to check each time we get control back from lock.wait().
                while (!S3_DATA_REQUESTS[index].isRequestComplete()) {
                    try {
                        LOG.info("About to do wait for " + index);
                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                bos.write(S3_DATA_REQUESTS[index].getS3Data());
            } catch (IOException e) {
                e.printStackTrace();
            }

            digest.update(S3_DATA_REQUESTS[index].getS3Data());

            LOG.info(
                    "Got complete for index " + index + " at position " + S3_DATA_REQUESTS[index].getStartPosition()
                            + " and length " + S3_DATA_REQUESTS[index].getS3Data().length
                            + ". currentFilePosition is " + currentFilePosition);
            currentFilePosition += S3_DATA_REQUESTS[index].getS3Data().length;

            doNextRequest(index);
            index++;
            if (index >= S3_DATA_REQUESTS.length) {
                index = 0;
            }
        }
        try {
            bos.flush();
            bos.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[] bytes = digest.digest();
        return GetMD5.digestDecimalToHex(bytes);
    }

    /**
     * Request the next segment of the file associating it with position <code>index</code>
     * in <code>S3_DATA_REQUESTS</code>.
     *
     * @param index
     */
    private void doNextRequest(int index) {
        if (requestedFileCurrentPosition < fileSize) {
            int requestSize = requestedFileCurrentPosition + requestLength < fileSize ?
                    requestLength : (int)(fileSize - requestedFileCurrentPosition);
            S3_DATA_REQUESTS[index] = new S3DataRequest(awsS3Client,
                                                        requestedFileCurrentPosition,
                                                        requestSize,
                                                        bucketName,
                                                        key,
                                                        index,
                                                        lock);
            MY_EXECUTOR.execute(new S3DataReader(S3_DATA_REQUESTS[index]));
            requestedFileCurrentPosition += requestSize;
        }
    }

    /**
     *
     * @return
     */
    private int initBufferRequest() {
        int index;

        for (index=0; index<THREAD_POOL_SIZE; index++) {
            if (requestedFileCurrentPosition + requestLength > fileSize) {
                break;
            }
            S3_DATA_REQUESTS[index] = new S3DataRequest(awsS3Client,
                                                        requestedFileCurrentPosition,
                                                        requestLength,
                                                        bucketName,
                                                        key,
                                                        index,
                                                        lock);
            requestedFileCurrentPosition += requestLength;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // We don't care if we are interrupted!
            }
        }
        if (requestedFileCurrentPosition < fileSize && index != THREAD_POOL_SIZE) {
            S3_DATA_REQUESTS[index++] = new S3DataRequest(awsS3Client,
                                                          requestedFileCurrentPosition,
                                                          (int)(fileSize - requestedFileCurrentPosition),
                                                          bucketName,
                                                          key,
                                                          index,
                                                          lock);
        }

        for (int i=0;i<index;i++) {
            MY_EXECUTOR.execute(new S3DataReader(S3_DATA_REQUESTS[i]));
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
        MulitPartDownloadInputStream mpdis = new MulitPartDownloadInputStream();

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
     * @param profileName
     */
    private static void setupAWS(String profileName) {
        credentialsProvider = new ProfileCredentialsProvider(profileName);
        awsS3Client = new AmazonS3Client(credentialsProvider);
    }

    /**
     *
     * @param bucketName
     * @param key
     * @return
     */
    private static long getObjectSizePlusMetadata(String bucketName, String key) {
        GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(bucketName, key);
        objectMetadata = awsS3Client.getObjectMetadata(getObjectMetadataRequest);
        return objectMetadata.getContentLength();
    }

    /**
     * Helper method to get the MD5 checksum.
     *
     * NOTE: uses ETAG unless it contains a '-' as that means it is incorrect due to multipart upload.
     * Will also look for
     * s3cmd-attrs - search for md5:######
     *
     * @return MD5 hash if a valid one can be found or null otherwise.
     */
    private static String getMD5() {
        String md5 = objectMetadata.getETag();
        if (md5.contains("-")) {
            LOG.info("ETag contained a '-' so was generated from multipart upload, looking for alternative MD5");
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
                        LOG.info("Using s3cmd-attrs user metadata md5 hash");
                        md5 = keyVal[1];
                    }
                }
            }
        }
        if (md5 != null && md5.length() != 32) {
            LOG.error("MD5 checksum was not 32 characters long!");
            md5 = null;
        }
        return md5;
    }

    public static void main(String[] args) throws Exception {
        String bucketName = "a-c-test";
        String key = "t1";
        String destinationPath;
        int threadBufferSize = DEFAULT_REQUEST_LENGTH;

        String profileName = null;

        Option awsProfile = Option.builder("aws_profile")
                .hasArg()
                .argName("profile_name")
                .desc("the aws profile to use")
                .build();

        Option threadBuffer = Option.builder("thread_buffer")
                .hasArg()
                .argName("SIZE")
                .desc("number of bytes in each thread's buffer, size of each download part")
                .build();

        Options options = new Options();
        options.addOption(awsProfile);
        options.addOption(threadBuffer);

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
                LOG.info("Found thread_buffer option with value " + value.toString());
                threadBufferSize = Integer.parseInt(value);
                LOG.debug("Option is integer with value " + threadBufferSize);
            }

            List<String> mainArguments = line.getArgList();
            if (mainArguments.size() == 3) {
                bucketName = mainArguments.get(0);
                key = mainArguments.get(1);
                destinationPath = mainArguments.get(2);
            }
            else {
                // automatically generate the help statement
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "CopyFileFromS3 [OPTIONS] <Bucket Name> <Object Name> <Destination File>", options );
                return;
            }
        } catch (ParseException exp) {
            // oops, something went wrong
            LOG.error( "Parsing failed.", exp);

            // automatically generate the help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "CopyFileFromS3", options );
            return;
        }


        setupAWS(profileName);

        // Start timer here so we get time of file transfer without setup time being added
        long startTime = System.currentTimeMillis();

        long fileSize = getObjectSizePlusMetadata(bucketName, key);

        CopyFileFromS3 me = new CopyFileFromS3(fileSize, threadBufferSize, bucketName, key, destinationPath);
        String downloadChecksum = me.go();
        String md5 = getMD5();
        LOG.info("Checksums " + (downloadChecksum.matches(md5) ? "matches" : "does not match")
                + " MD5 hash is " + downloadChecksum + " and original file was " + md5);
        long finishTime = System.currentTimeMillis();
        // me.test1();
        LOG.info("At end of main, about to shutdown Executor");

        // Shut down the threads
        MY_EXECUTOR.shutdown();
        if (!MY_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
            LOG.warn("Forcing Executor shutdown");
            MY_EXECUTOR.shutdownNow();
        }
        LOG.info("Finished in " + (finishTime - startTime) + ".");
    }
}
