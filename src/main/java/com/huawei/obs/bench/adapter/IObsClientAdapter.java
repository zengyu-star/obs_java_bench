package com.huawei.obs.bench.adapter;

import java.nio.ByteBuffer;

/**
 * OBS Client Adapter Interface (Execution Plane - Adapter Interface)
 * Responsibility: Defines standard operations required for benchmarking, shielding underlying SDK differences.
 * Architectural Requirement: Return values must be HTTP status codes (int) for high-speed statistics.
 */
public interface IObsClientAdapter {

    /**
     * TestCase 201: Simple Object Upload (PUT Object)
     * @param bucketName Target bucket name
     * @param objectKey Target object key
     * @param payload Data stream to upload (Direct ByteBuffer)
     * @return HTTP status code (e.g., 200, 403, 500)
     */
    int putObject(String bucketName, String objectKey, ByteBuffer payload);

    /**
     * TestCase 202: Simple Object Download (GET Object)
     * @param bucketName Target bucket name
     * @param objectKey Target object key
     * @param range Download range (e.g., "bytes=0-1024"); if null, performs full download
     * @param expectedPattern Expected bitstream for data verification (Pattern Buffer)
     * @param receiveBuffer Pre-allocated buffer for consumption and verification
     * @return HTTP status code (e.g., 200, 206, 404, or 600 for data corruption)
     */
    int getObject(String bucketName, String objectKey, String range, ByteBuffer expectedPattern, ByteBuffer receiveBuffer);

    /**
     * TestCase 204: Delete Object (DELETE Object)
     * @param bucketName Target bucket name
     * @param objectKey Target object key
     * @return HTTP status code (e.g., 204)
     */
    int deleteObject(String bucketName, String objectKey);

    /**
     * TestCase 216: Multipart Upload (Multipart Upload)
     * Internal Logic: Init -> UploadParts -> Complete
     * @param bucketName Target bucket name
     * @param objectKey Target object key
     * @param payload Complete data stream to upload
     * @param partCount Total number of parts
     * @param partSize Size of each part
     * @return Final status code
     */
    int multipartUpload(String bucketName, String objectKey, ByteBuffer payload, int partCount, long partSize);

    /**
     * TestCase 230: Resumable Upload (Resumable Upload)
     * Uses high-level SDK interfaces for concurrent large file uploads.
     * @param bucketName Target bucket name
     * @param objectKey Target object key
     * @param filePath Path to local file
     * @param taskNum Concurrency thread count for the task
     * @param partSize Part size
     * @param enableCheckpoint Whether to enable resumable checkpoints
     * @return Final status code
     */
    int resumableUpload(String bucketName, String objectKey, String filePath, int taskNum, long partSize, boolean enableCheckpoint);

    /**
     * Fetches the latest OBS RequestId (for detailed log tracing)
     */
    String getLastRequestId();
}
