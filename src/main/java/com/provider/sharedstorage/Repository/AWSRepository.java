package com.provider.sharedstorage.Repository;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.springframework.stereotype.Repository;


import java.io.File;
import java.nio.file.Files;
import java.util.List;

@Repository
public class AWSRepository {
    public AWSRepository() {

    }

    public BasicSessionCredentials assumeRole(String roleArn) {
        // String roleARN = "arn:aws:iam::844102931058:role/test_AssumeRole_VCM";
        AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(Regions.US_EAST_1)
                .build();

        System.out.println("hello");

        // Obtain credentials for the IAM role. Note that you cannot assume the role of an AWS root account;
        // Amazon S3 will deny access. You must use credentials for an IAM user or an IAM role.
        com.amazonaws.services.securitytoken.model.AssumeRoleRequest roleRequest = new AssumeRoleRequest()
                .withRoleArn(roleArn)
                .withRoleSessionName("session")
                .withDurationSeconds(900);
        AssumeRoleResult roleResponse;
        try {
             roleResponse = stsClient.assumeRole(roleRequest);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        Credentials sessionCredentials = roleResponse.getCredentials();

        // Create a BasicSessionCredentials object that contains the credentials you just retrieved.
        BasicSessionCredentials awsCredentials = new BasicSessionCredentials(
                sessionCredentials.getAccessKeyId(),
                sessionCredentials.getSecretAccessKey(),
                sessionCredentials.getSessionToken());

        return awsCredentials;
    }


    // This function will verify if there is the dataset available or not
    // will throw an error in case the dataset is not present.

    public JSONObject createDataset (BasicSessionCredentials awsCredentials, JSONArray datasets) {

        JSONObject output = new JSONObject();
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                    .withRegion(Regions.US_EAST_2)
                    .build();

            for (Object o: datasets) {
                JSONObject dataset = (JSONObject) o;
                if (s3Client.doesBucketExistV2(dataset.getAsString("location"))) {
                    output.put("message", "created the datasets");
                    output.put("statusCode", 200);
                    return output;
                } else {
                    output.put("message", "Failed to create the datasets because the bucket doesn't exist");
                    output.put("statusCode", 500);
                    return output;
                }
            }
            output.put("message", "created the datasets");
            output.put("statusCode", 200);
        } catch (Exception e) {
            e.printStackTrace();
            output.put("message", "Failed to create the datasets");
            output.put("error", e.getMessage());
            output.put("statusCode", 500);
        }
        return output;

    }

    public Object addAssets(BasicSessionCredentials awsCredentials, String bucket, String fileName) {
        System.out.println(fileName);
        JSONObject output = new JSONObject();
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                    .withRegion(Regions.US_EAST_2)
                    .build();

            PutObjectResult putObjectResult = s3Client.putObject(bucket, fileName, new File(fileName));
            output.put("message", "Uploaded the Assets");
            output.put("statusCode", 200);
        } catch (Exception e) {
            e.printStackTrace();
            output.put("message", "Failed to upload the assets");
            output.put("Error", e.getMessage());
            output.put("statusCode", 500);
        }
        return output;
    }

    public Object downloadAssets(BasicSessionCredentials awsCredentials, String bucket) {
        JSONObject output = new JSONObject();
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                    .withRegion(Regions.US_EAST_2)
                    .build();

            System.out.println(bucket);

            JSONArray objects = (JSONArray) this.getAssets(awsCredentials, bucket);

            for ( Object o: objects) {
                JSONObject s3Object = (JSONObject) o;
                File localFile = new File("downloaded_" + s3Object.getAsString("name"));

                ObjectMetadata object = s3Client.getObject(new GetObjectRequest(bucket, s3Object.getAsString("name")), localFile);
            }

            output.put("message", "Downloaded the Assets");
            output.put("statusCode", 200);
        } catch (Exception e) {
            e.printStackTrace();
            output.put("message", "Failed to download the assets");
            output.put("Error", e.getMessage());
            output.put("statusCode", 500);
        }
        return output;
    }

    public Object deleteBucket(String bucket, BasicSessionCredentials awsCredentials) {
        JSONObject output = new JSONObject();
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                    .withRegion(Regions.US_EAST_1)
                    .build();

            s3Client.deleteBucket(bucket);

            output.put("message", "Deleted the dataset");
            output.put("statusCode", 200);
        } catch (Exception e) {
            e.printStackTrace();
            output.put("message", "Failed to delete the dataset");
            output.put("statusCode", 500);
        }
        return output;
    }

    public Object getAssets(BasicSessionCredentials awsCredentials, String bucket) {
        JSONArray output = new JSONArray();
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                    .withRegion(Regions.US_EAST_2)
                    .build();

            ObjectListing listObjectsResponse = s3Client.listObjects(bucket);
            List<S3ObjectSummary> s3objects =listObjectsResponse.getObjectSummaries();
            s3objects.forEach( s3object -> {
                JSONObject s3 = new JSONObject();
                s3.put("name", s3object.getKey());
                s3.put("lastModified", s3object.getLastModified());
                output.add(s3);
            });

        } catch (Exception e) {
            e.printStackTrace();

        }
        return output;
    }
}
