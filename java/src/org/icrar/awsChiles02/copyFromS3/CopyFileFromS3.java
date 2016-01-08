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


import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 */
public class CopyFileFromS3 {
    private static final int THREAD_POOL_SIZE = 5;
    private static final int REQUEST_LENGTH = 6;//1024;
    private static final int FILE_SIZE = 62;// * 1024 + 10;
    private static final String BUCKET_NAME = "a-c-test";
    private static final String KEY_NAME = "t1";
    private static final String DESTINATION_NAME = "/Users/mboulton/t1.out";
    private static final Log LOG = LogFactory.getLog(CopyFileFromS3.class);
    private static final ExecutorService MY_EXECUTOR = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    private static final S3DataRequest[] S3_DATA_REQUESTS = new S3DataRequest[THREAD_POOL_SIZE];

    private static ProfileCredentialsProvider credentialsProvider;
    private static AmazonS3Client awsS3Client;

    private static int requestedFileCurrentPosition = 0;

    // This lock is used to sync between the Threads so we know when they are ready.
    private static final class Lock { }
    private final Object lock = new Lock();

    private CopyFileFromS3() {}

    /**
     * Do the actual download of the S3 file.
     */
    private void go() {
        int bufferElementsLoaded = initBufferRequest(FILE_SIZE);
        int currentFilePosition = 0;
        int index = 0;

        File outFile = new File(DESTINATION_NAME);
        if (outFile.exists()) {
            System.out.println("Output file " + outFile.getAbsolutePath() + " exists, overwritting!");
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

        while (currentFilePosition < FILE_SIZE) {
            synchronized (lock) {
                // Loop here until the next segmen of the file is ready. We can't wait on a particular thread
                // hence the reason to check each time we get control back from lock.wait().
                while (!S3_DATA_REQUESTS[index].isRequestComplete()) {
                    try {
                        System.out.println("About to do wait");
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

            System.out.println(
                    "Got complete for index " + index + " at position " + S3_DATA_REQUESTS[index].getStartPosition()
                            + " and length " + S3_DATA_REQUESTS[index].getS3Data().length);
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
    }

    /**
     * Request the next segment of the file associating it with position <code>index</code>
     * in <code>S3_DATA_REQUESTS</code>.
     *
     * @param index
     */
    private void doNextRequest(int index) {
        if (requestedFileCurrentPosition < FILE_SIZE) {
            int requestSize = requestedFileCurrentPosition + REQUEST_LENGTH < FILE_SIZE ?
                    REQUEST_LENGTH : FILE_SIZE - requestedFileCurrentPosition;
            S3_DATA_REQUESTS[index] = new S3DataRequest(awsS3Client,
                                                        requestedFileCurrentPosition,
                                                        requestSize,
                                                        BUCKET_NAME,
                                                        KEY_NAME,
                                                        lock);
            MY_EXECUTOR.execute(new S3DataReader(S3_DATA_REQUESTS[index]));
            requestedFileCurrentPosition += requestSize;
        }
    }

    /**
     *
     * @param fileSize
     * @return
     */
    private int initBufferRequest(final int fileSize) {
        int index;

        for (index=0; index<THREAD_POOL_SIZE; index++) {
            if (requestedFileCurrentPosition + REQUEST_LENGTH > fileSize) {
                break;
            }
            S3_DATA_REQUESTS[index] = new S3DataRequest(awsS3Client, requestedFileCurrentPosition, REQUEST_LENGTH, BUCKET_NAME, KEY_NAME, lock);
            requestedFileCurrentPosition += REQUEST_LENGTH;
        }
        if (requestedFileCurrentPosition < fileSize && index != THREAD_POOL_SIZE) {
            S3_DATA_REQUESTS[index++] = new S3DataRequest(awsS3Client, requestedFileCurrentPosition, fileSize- requestedFileCurrentPosition, BUCKET_NAME, KEY_NAME, lock);
        }

        for (int i=0;i<index;i++) {
            MY_EXECUTOR.execute(new S3DataReader(S3_DATA_REQUESTS[i]));
        }

        return index;
    }

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
    }

    private static void setupAWS(String profileName) {
        credentialsProvider = new ProfileCredentialsProvider(profileName);
        awsS3Client = new AmazonS3Client(credentialsProvider);
    }

    public static void main(String[] args) throws Exception {
        String bucketName = "a-c-test";
        String filePath = "t1";
        String keyName;
        String profileName = "aws-profile";

        setupAWS(profileName);

        CopyFileFromS3 me = new CopyFileFromS3();
        me.go();
        // me.test1();
        System.out.println("At end of main, about to shutdown Executor");
        MY_EXECUTOR.shutdown();
        if (!MY_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
            System.out.println("Forcing Executor shutdown");
            MY_EXECUTOR.shutdownNow();
        }
        System.out.println("Finished.");
    }
}
