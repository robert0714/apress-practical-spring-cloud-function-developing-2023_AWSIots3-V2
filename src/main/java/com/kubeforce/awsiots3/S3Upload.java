package com.kubeforce.awsiots3;
import java.io.IOException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.S3Client;


public class S3Upload {



     public String S3upload(String payload) {
         //set-up the client
         Region region = Region.US_WEST_2;
         S3Client s3 = S3Client.builder().region(region).build();

         String bucketName = "greengrass";

         String key = "IoT";

         s3.putObject(PutObjectRequest.builder().bucket(bucketName).key(key)
                         .build(),
                 RequestBody.fromString(payload));
         s3.close();
        return ("success");
     }

}
