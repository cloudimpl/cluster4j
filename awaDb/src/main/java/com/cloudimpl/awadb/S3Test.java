/*
 * Copyright 2021 nuwan.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudimpl.awadb;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.hubject.aws.s3.io.S3OutputStream;
import com.hubject.aws.s3.io.SimpleByteBufferPool;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author nuwan
 */
public class S3Test {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        AmazonS3 client = AmazonS3Client.builder().withCredentials(new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return new AWSCredentials() {  

                    @Override
                    public String getAWSAccessKeyId() {
                        return "Z6NFGJMQJZXUFVJ33OAG";
                    }

                    @Override
                    public String getAWSSecretKey() {
                        return "Bk2yG7gKEJBlWdR9MdN1QhbL9ln9HARaWRdSSYIQ";
                    }
                };
            }

            @Override
            public void refresh() {

            }
        })
                
      //  .withRegion(Regions.US_EAST_1)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("s3.us-east-1.wasabisys.com", "us-east-1"))
        .        build();

     //   client.putObject("test.cloudimpl.com", "test", "test");
        try (FileInputStream in = new FileInputStream(new File("/Users/nuwan/data/0.xlog"))) {
            try (S3OutputStream stream = new S3OutputStream(client, "test.cloudimpl.com", "test2", 10485760, false, new SimpleByteBufferPool(true))) {
                byte[] buffer = new byte[8192];
                int n;
                while ((n = in.read(buffer)) != -1) {
                    stream.write(buffer, 0, n);
                }
            }
        }
    }
}
