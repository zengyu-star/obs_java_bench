package com.huawei.obs.bench.adapter;

import com.huawei.obs.bench.utils.ByteBufferInputStream;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.*;

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Real OBS Client Adapter (Execution Plane - Real Implementation)
 * Responsibility: Interfaces with the official Huawei Cloud Java SDK to execute physical network requests.
 */
public class RealObsAdapter implements IObsClientAdapter {

    private final ObsClient obsClient;
    private String lastRequestId = ""; // [New]: Track last request ID for detail logging
    private final ThreadLocal<Long> lastRequestBytes = ThreadLocal.withInitial(() -> 0L);

    public RealObsAdapter(ObsClient obsClient) {
        this.obsClient = obsClient;
    }

    @Override
    public long getLastRequestBytes() {
        return lastRequestBytes.get();
    }

    @Override
    public String getLastRequestId() {
        return lastRequestId;
    }

    @Override
    public int createBucket(String bucketName, String location) {
        try {
            CreateBucketRequest request = new CreateBucketRequest();
            request.setBucketName(bucketName);
            
            // [Align with Demo]: Set standard bucket attributes
            request.setAcl(AccessControlList.REST_CANNED_PRIVATE);
            request.setBucketStorageClass(StorageClassEnum.STANDARD);
            
            if (location != null && !location.isBlank()) {
                request.setLocation(location);
            }
            
            // [Note]: Some regions/accounts might require explicit AZ setting. 
            // Default is usually single AZ, demo uses MULTI_AZ.
            // request.setAvailableZone(AvailableZoneEnum.MULTI_AZ); 

            ObsBucket result = obsClient.createBucket(request);
            this.lastRequestId = result.getRequestId();
            lastRequestBytes.set(0L);
            return result.getStatusCode();
        } catch (ObsException e) {
            this.lastRequestId = e.getErrorRequestId();
            return e.getResponseCode();
        } catch (Exception e) {
            this.lastRequestId = "CLIENT_ERROR";
            return 0;
        }
    }

    @Override
    public int putObject(String bucketName, String objectKey, ByteBuffer payload) {
        try {
            PutObjectRequest request = new PutObjectRequest();
            request.setBucketName(bucketName);
            request.setObjectKey(objectKey);
            
            // [Architect Optimization]: Pass Direct ByteBuffer to SDK via custom stream to avoid byte[] copying
            if (payload != null) {
                request.setInput(new ByteBufferInputStream(payload));
            }

            PutObjectResult result = obsClient.putObject(request);
            this.lastRequestId = result.getRequestId();
            lastRequestBytes.set(payload != null ? (long) payload.limit() : 0L);
            return result.getStatusCode();

        } catch (ObsException e) {
            this.lastRequestId = e.getErrorRequestId();
            return e.getResponseCode();
        } catch (Exception e) {
            this.lastRequestId = "CLIENT_ERROR";
            return 0;
        }
    }

    @Override
    public int getObject(String bucketName, String objectKey, String range, ByteBuffer expectedPattern, ByteBuffer receiveBuffer) {
        try {
            GetObjectRequest request = new GetObjectRequest(bucketName, objectKey);
            if (range != null && !range.isBlank()) {
                request.setRangeStart(parseRangeStart(range));
                request.setRangeEnd(parseRangeEnd(range));
            }

            ObsObject obsObject = obsClient.getObject(request);
            long contentLength = obsObject.getMetadata() != null ? obsObject.getMetadata().getContentLength() : 0;
            this.lastRequestId = (obsObject.getMetadata() != null) ? obsObject.getMetadata().getRequestId() : "UNKNOWN";
            
            // [Critical]: InputStream must be fully consumed and closed to prevent connection pool exhaustion.
            // Also performs Zero-GC data validation here.
            boolean[] isValidResult = new boolean[]{true};
            long bytesRead = consumeAndVerifyStream(obsObject.getObjectContent(), expectedPattern, receiveBuffer, isValidResult);
            
            // Explicitly validate length if we expect validation
            if (isValidResult[0] && expectedPattern != null && contentLength > 0 && bytesRead != contentLength) {
                System.err.printf("[ERROR] Data length validation failed! Expected %d but got %d.\n", contentLength, bytesRead);
                isValidResult[0] = false;
            }
            
            lastRequestBytes.set(isValidResult[0] ? bytesRead : 0L);
            
            return isValidResult[0] ? 200 : 600;

        } catch (ObsException e) {
            this.lastRequestId = e.getErrorRequestId();
            return e.getResponseCode();
        } catch (Exception e) {
            this.lastRequestId = "CLIENT_ERROR";
            return 0;
        }
    }

    @Override
    public int deleteObject(String bucketName, String objectKey) {
        try {
            DeleteObjectResult result = obsClient.deleteObject(bucketName, objectKey);
            this.lastRequestId = result.getRequestId();
            lastRequestBytes.set(0L);
            return result.getStatusCode();
        } catch (ObsException e) {
            this.lastRequestId = e.getErrorRequestId();
            return e.getResponseCode();
        } catch (Exception e) {
            this.lastRequestId = "CLIENT_ERROR";
            return 0;
        }
    }

    @Override
    public int multipartUpload(String bucketName, String objectKey, ByteBuffer payload, int partCount, long partSize) {
        try {
            // 1. Initialize multipart upload
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, objectKey);
            InitiateMultipartUploadResult initResult = obsClient.initiateMultipartUpload(initRequest);
            String uploadId = initResult.getUploadId();

            // 2. Loop through parts (Simple version: sequential upload; can be parallelized per thread if needed)
            java.util.List<PartEtag> etags = new java.util.ArrayList<>();
            for (int i = 1; i <= partCount; i++) {
                UploadPartRequest partRequest = new UploadPartRequest();
                partRequest.setBucketName(bucketName);
                partRequest.setObjectKey(objectKey);
                partRequest.setUploadId(uploadId);
                partRequest.setPartNumber(i);
                
                // Slice simulation: Reset position and read fixed length
                payload.position((int) ((i - 1) * partSize));
                partRequest.setInput(new ByteBufferInputStream(payload));
                partRequest.setPartSize(partSize);
                
                UploadPartResult partResult = obsClient.uploadPart(partRequest);
                etags.add(new PartEtag(partResult.getEtag(), partResult.getPartNumber()));
            }

            // 3. Complete multipart upload
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(bucketName, objectKey, uploadId, etags);
            CompleteMultipartUploadResult result = obsClient.completeMultipartUpload(completeRequest);
            lastRequestBytes.set((long) partCount * partSize);
            return result.getStatusCode();

        } catch (ObsException e) {
            return e.getResponseCode();
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    public int resumableUpload(String bucketName, String objectKey, String filePath, int taskNum, long partSize, boolean enableCheckpoint) {
        try {
            // Use SDK's high-level resumable upload interface
            UploadFileRequest request = new UploadFileRequest(bucketName, objectKey);
            request.setUploadFile(filePath);
            request.setTaskNum(taskNum);
            request.setPartSize(partSize);
            request.setEnableCheckpoint(enableCheckpoint);

            CompleteMultipartUploadResult result = obsClient.uploadFile(request);
            lastRequestBytes.set(0L);
            return result.getStatusCode();
        } catch (ObsException e) {
            return e.getResponseCode();
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * High-performance stream consumption with optional Zero-GC pattern verification
     * @return Number of bytes processed
     */
    private long consumeAndVerifyStream(InputStream in, ByteBuffer expectedPattern, ByteBuffer receiveBuffer, boolean[] outIsValid) {
        if (in == null) {
            outIsValid[0] = false;
            return 0;
        }
        
        boolean isValidationEnabled = expectedPattern != null && receiveBuffer != null;
        boolean isValid = true;
        int patternSize = isValidationEnabled ? expectedPattern.capacity() : 0;
        long bytesProcessed = 0;

        try (in) {
            if (isValidationEnabled) {
                // Consistency validation enabled
                byte[] underlyingReadArray;
                int readCapacity;
                
                // If DirectBuffer without backing array, use a small heap buffer for transfer
                if (receiveBuffer.hasArray()) {
                    underlyingReadArray = receiveBuffer.array();
                    readCapacity = receiveBuffer.capacity();
                } else {
                    readCapacity = receiveBuffer.capacity();
                    underlyingReadArray = new byte[readCapacity]; // Small initial allocation; can be optimized by Escape Analysis
                }

                int bytesRead;
                byte[] expectedArray = expectedPattern.hasArray() ? expectedPattern.array() : null;

                while ((bytesRead = in.read(underlyingReadArray, 0, readCapacity)) != -1) {
                    if (!isValid) continue; // After corruption, just drain to prevent pool leaks
                    
                    for (int i = 0; i < bytesRead; i++) {
                        int expectedIndex = (int) (bytesProcessed % patternSize);
                        byte expectedByte = expectedArray != null ? expectedArray[expectedIndex] : expectedPattern.get(expectedIndex);
                        
                        if (underlyingReadArray[i] != expectedByte) {
                            System.err.printf("[ERROR] Data integrity validation failed! Inconsistency detected at offset %d.\n", bytesProcessed);
                            isValid = false;
                            break;
                        }
                        bytesProcessed++;
                    }
                }
            } else {
                // Drain as fast as possible without validation
                byte[] drain = new byte[8192];
                int bytesRead;
                while ((bytesRead = in.read(drain)) != -1) {
                    bytesProcessed += bytesRead;
                }
            }
        } catch (Exception ignore) {
            outIsValid[0] = false;
            return bytesProcessed;
        }
        
        outIsValid[0] = isValid;
        return bytesProcessed;
    }

    // Simple Range parsing logic (Example: "0-1024")
    private long parseRangeStart(String range) {
        return Long.parseLong(range.split("-")[0]);
    }
    private long parseRangeEnd(String range) {
        String[] parts = range.split("-");
        return parts.length > 1 ? Long.parseLong(parts[1]) : -1L;
    }
}
