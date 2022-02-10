package com.modak.utils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.io.FilenameUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class S3Utils {

    public static AmazonS3 getS3ClientIAM() {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard() .withCredentials(new InstanceProfileCredentialsProvider(false)) .build();
        return s3;
    }

    public static AmazonS3 getS3Client(String accessKey, String secretKey, Regions defaultRegion) {
        BasicAWSCredentials credentials = new BasicAWSCredentials(
                accessKey, secretKey
        );
        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(defaultRegion)
                .build();
        return s3Client;
    }

    public static List<String> listObjects(AmazonS3 s3Client, String bucketName, String prefix) {
        if (s3Client== null || bucketName == null) {
            throw new NullPointerException("parameters are null");
        }
        if ( prefix == null) {
            prefix = "";
        }
        ArrayList<String> list = new ArrayList<>();
        S3Objects.withPrefix(s3Client, bucketName,prefix).withBatchSize(100).forEach((S3ObjectSummary objectSummary) -> {
            list.add(objectSummary.getKey());
        });

        return list;
    }


    public static List<String> listObjects(AmazonS3 s3Client, String bucketName, String prefix, String regexMatchPattern) {
        List<String> list = listObjects(s3Client,bucketName,prefix);
        ArrayList<String> rList = new ArrayList<>();
        Pattern p = Pattern.compile(regexMatchPattern); // try this first to see if it is correct
        p = Pattern.compile(".*" + regexMatchPattern + ".*"); // we try to match the whole string hence the additions
        for (String key : list) {
            if (p.matcher(key).matches()) {
                rList.add(key);
            }
        }
        return rList;
    }


    public static void deleteObjects(AmazonS3 s3Client, String sourceBucketName, String prefix) {
        List<String> list = listObjects(s3Client,sourceBucketName,prefix);
        for (String key: list) {
            deleteObject(s3Client,sourceBucketName,key);
        }
    }

    public static void deleteObject(AmazonS3 s3Client, String sourceBucketName, String sourceObjectKey) {
        ObjectMetadata objectMetadata = s3Client.getObjectMetadata(sourceBucketName,sourceObjectKey);
        if (objectMetadata != null) {
            s3Client.deleteObject(new DeleteObjectRequest(sourceBucketName, sourceObjectKey));
        }
    }

    public static void deleteObject(AmazonS3 s3Client, String s3URL) {
        if (s3URL != null) {
            AmazonS3URI as3uri = new AmazonS3URI(s3URL);
            if (as3uri != null && as3uri.getBucket() != null & as3uri.getKey() != null)  {
                s3Client.deleteObject(new DeleteObjectRequest(as3uri.getBucket(), as3uri.getKey()));
            }
        }

    }


    public static void moveObject(AmazonS3 s3Client, String sourceBucketName, String sourceObjectKey,
                                  String destBucketName,  String destObjectKey) {
        if (s3Client== null || sourceBucketName == null
                || destBucketName == null || sourceObjectKey == null || destObjectKey == null ) {
            throw new NullPointerException("parameters are null");
        }
        ObjectMetadata objectMetadata = s3Client.getObjectMetadata(sourceBucketName,sourceObjectKey);
        if (objectMetadata != null) {
            long inputSize = objectMetadata.getContentLength();
            if (inputSize >= 1024*1024*1024) {
                // size greater than 1GB, let's use multipartload
                multipartUpload(s3Client, sourceBucketName,
                        destBucketName, sourceObjectKey, destObjectKey);
            }
            else {
                CopyObjectRequest copyObjRequest = new CopyObjectRequest(sourceBucketName, sourceObjectKey
                        , destBucketName, destObjectKey);
                s3Client.copyObject(copyObjRequest);
            }
            ObjectMetadata objectMetadata2 = s3Client.getObjectMetadata(destBucketName,destObjectKey);
            if (objectMetadata2 != null) {
                long outputSize = objectMetadata2.getContentLength();
                if (inputSize != outputSize) {
                    throw new AmazonServiceException("Copy failed. The size in destination doesn't match source object size.");
                }
            }
            else {
                throw new AmazonServiceException("Copy failed");
            }
        }
        else {
            throw new AmazonServiceException(sourceObjectKey + " object not found in " + sourceBucketName + " bucket.");
        }

    }

    private static void multipartUpload(AmazonS3 s3Client, String sourceBucketName,
                                       String destBucketName, String sourceObjectKey, String destObjectKey)  {
        try {

            // Initiate the multipart upload.
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(destBucketName, destObjectKey);
            InitiateMultipartUploadResult initResult = s3Client.initiateMultipartUpload(initRequest);

            // Get the object size to track the end of the copy operation.
            GetObjectMetadataRequest metadataRequest = new GetObjectMetadataRequest(sourceBucketName, sourceObjectKey);
            ObjectMetadata metadataResult = s3Client.getObjectMetadata(metadataRequest);
            long objectSize = metadataResult.getContentLength();

            // Copy the object using 5 MB parts.
            long partSize = 5 * 1024 * 1024;
            long bytePosition = 0;
            int partNum = 1;
            List<CopyPartResult> copyResponses = new ArrayList<CopyPartResult>();
            while (bytePosition < objectSize) {
                // The last part might be smaller than partSize, so check to make sure
                // that lastByte isn't beyond the end of the object.
                long lastByte = Math.min(bytePosition + partSize - 1, objectSize - 1);

                // Copy this part.
                CopyPartRequest copyRequest = new CopyPartRequest()
                        .withSourceBucketName(sourceBucketName)
                        .withSourceKey(sourceObjectKey)
                        .withDestinationBucketName(destBucketName)
                        .withDestinationKey(destObjectKey)
                        .withUploadId(initResult.getUploadId())
                        .withFirstByte(bytePosition)
                        .withLastByte(lastByte)
                        .withPartNumber(partNum++);
                copyResponses.add(s3Client.copyPart(copyRequest));
                bytePosition += partSize;
            }

            // Complete the upload request to concatenate all uploaded parts and make the copied object available.
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                    destBucketName,
                    destObjectKey,
                    initResult.getUploadId(),
                    getETags(copyResponses));
            s3Client.completeMultipartUpload(completeRequest);
            System.out.println("Multipart copy complete.");
        } catch (AmazonServiceException e) {
            throw e;
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            throw e;
        }
    }

    public static void consolidateSparkOutputFiles(AmazonS3 s3Client, String s3URL) throws Exception {
        if (s3URL != null) {
            AmazonS3URI as3uri = new AmazonS3URI(s3URL);
            if (as3uri != null) {
                String bucket = as3uri.getBucket();
                String key = as3uri.getKey();
                if (bucket != null & key != null) {
                    String prefix = key + "/";
                    String extension = FilenameUtils.getExtension(key);
                    if (extension == null) throw new Exception("URL needs to have an extension. e.g. CSV or Parquet");
                    List<String> list = listObjects(s3Client, bucket, prefix, "part-.*\\." + extension);
                    if (list.size() == 1) {
                        //move the file
                        // e.g bucket/folder/myfile.csv/part-nslnvlsns13r83-.csv will be moved to bucket/folder/myfile.csv
                        moveObject(s3Client, bucket, list.get(0), bucket, key);
                        //Now delete the files
                        deleteObjects(s3Client, bucket, prefix);
                    } else if (list.size() > 1) {
                        throw new Exception("Only one file is expected to consolidate.");
                    } else {
                        throw new Exception("No file found to consolidate.");
                    }
                }
                else {
                    throw new Exception("URL doesn't seem to be valid");
                }
            }
        }
    }

    // This is a helper function to construct a list of ETags.
    private static List<PartETag> getETags(List<CopyPartResult> responses) {
        List<PartETag> etags = new ArrayList<PartETag>();
        for (CopyPartResult response : responses) {
            etags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return etags;
    }
}
