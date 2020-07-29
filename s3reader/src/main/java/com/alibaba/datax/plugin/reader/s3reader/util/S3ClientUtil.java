package com.alibaba.datax.plugin.reader.s3reader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.s3reader.Key;
import com.alibaba.datax.plugin.reader.s3reader.S3Reader;
import com.alibaba.datax.plugin.reader.s3reader.S3ReaderErrorCode;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * S3 client util
 *
 * @author L.cm
 */
public class S3ClientUtil {
    private static final Logger log = LoggerFactory.getLogger(S3Reader.Job.class);

    public static AmazonS3 initOssClient(Configuration conf) {
        String accessKey = conf.getString(Key.ACCESSKEY);
        String secretKey = conf.getString(Key.SECRETKEY);
        String region = conf.getString(Key.REGION);
        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");

        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://" + region, Regions.US_EAST_1.name()))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();

        return s3Client;
    }

    public static boolean doesBucketExist(AmazonS3 s3Client, String bucket) {
        try {
            return s3Client.doesBucketExist(bucket);
        } catch (Throwable e) {
            e.printStackTrace();
            log.warn("S3 bucket 不存在， 异常信息为：{}", e.getMessage());
            return false;
        }
    }

    public static String getBucketAcl(AmazonS3 s3Client, String bucket) {
        try {
            AccessControlList accessControlList = s3Client.getBucketAcl(bucket);
            return accessControlList.getOwner().getDisplayName();
        } catch (Throwable e) {
            log.warn("S3 bucket ack 异常， 异常信息为：{}", e.getMessage());
            return e.getMessage();
        }
    }

    public static S3Object getObject(AmazonS3 s3Client, String bucket, String object) {
        GetObjectRequest request = new GetObjectRequest(bucket,object);
        try {
            S3Object s3Object = s3Client.getObject(request);
            log.info("s3 file get success");
            return s3Object;
        } catch (Throwable e) {
            throw DataXException.asDataXException(S3ReaderErrorCode.S3_EXCEPTION, e.getMessage());
        }
    }

    public static List<String> listObjects(AmazonS3 s3Client, String bucket) {
        return listObjects(s3Client, bucket, null);
    }

    public static List<String> listObjects(AmazonS3 s3Client, String bucket, String prefix) {
        return listObjects(s3Client, bucket, prefix, null);
    }

    public static List<String> listObjects(AmazonS3 s3Client, String bucket, String prefix, String lastObject) {
        // 一次只拉取100个
        ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request();
        listObjectsV2Request.setBucketName(bucket);
        listObjectsV2Request.setMaxKeys(100);
        listObjectsV2Request.setPrefix(prefix);

        try {
            ListObjectsV2Result listObjects = s3Client.listObjectsV2(listObjectsV2Request);
            log.info("s3 file list success");
            return listObjects.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
        } catch (Throwable e) {
            log.error("S3文件列表拉取失败", e);
            return Collections.emptyList();
        }
    }

    public static void deleteObjects(AmazonS3 s3Client, String bucket, List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        List<DeleteObjectsRequest.KeyVersion> keyVersions = keys.stream()
                .map(key -> new DeleteObjectsRequest.KeyVersion(key))
                .collect(Collectors.toList());
        DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket);
        deleteObjectsRequest.setKeys(keyVersions);

        try {
            s3Client.deleteObjects(deleteObjectsRequest);
            log.info("s3 file delete success");
        } catch (Throwable e) {
            throw DataXException.asDataXException(S3ReaderErrorCode.S3_EXCEPTION, e.getMessage());
        }
    }

    public static void uploadObject(AmazonS3 s3Client, String bucket, String key, InputStream in, Long size) {
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucket,key,in,new ObjectMetadata());
        try {
            s3Client.putObject(putObjectRequest);
            log.info("s3 file upload success");
        } catch (Throwable e) {
            throw DataXException.asDataXException(S3ReaderErrorCode.S3_EXCEPTION, e.getMessage());
        }
    }

    public static UploadPartResult uploadPart(AmazonS3 s3Client, UploadPartRequest uploadPartRequest) {
        try {
            UploadPartResult uploadPartResponse = s3Client.uploadPart(uploadPartRequest);
            log.info("s3 file upload part success");
            return uploadPartResponse;
        } catch (Throwable e) {
            throw DataXException.asDataXException(S3ReaderErrorCode.S3_EXCEPTION, e.getMessage());
        }
    }

    public static String createMultipartUpload(AmazonS3 s3Client, String bucket, String currentObject) {
        InitiateMultipartUploadRequest uploadRequest = new InitiateMultipartUploadRequest(bucket,currentObject);
        try {
            InitiateMultipartUploadResult multipartUpload = s3Client.initiateMultipartUpload(uploadRequest);
            log.info("s3 file create upload multi part success");
            return multipartUpload.getUploadId();
        } catch (Throwable e) {
            throw DataXException.asDataXException(S3ReaderErrorCode.S3_EXCEPTION, e.getMessage());
        }
    }

    public static CompleteMultipartUploadResult completeMultipartUpload(AmazonS3 s3Client, String bucket, String currentObject,
                                                                        String uploadId, List<PartETag> partETags) {

        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest();
        request.setBucketName(bucket);
        request.setKey(currentObject);
        request.setUploadId(uploadId);
        request.setPartETags(partETags);

        try {
            CompleteMultipartUploadResult response = s3Client.completeMultipartUpload(request);
            log.info("s3 file complete upload multi part success");
            return response;
        } catch (Throwable e) {
            throw DataXException.asDataXException(S3ReaderErrorCode.S3_EXCEPTION, e.getMessage());
        }
    }

}
