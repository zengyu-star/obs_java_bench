package com.huawei.obs.bench.adapter;

import com.huawei.obs.bench.utils.ByteBufferInputStream;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.*;

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * 真实的 OBS 客户端适配器 (Execution Plane - Real Implementation)
 * 职责：对接华为云官方 Java SDK，执行真实的物理网络请求。
 */
public class RealObsAdapter implements IObsClientAdapter {

    private final ObsClient obsClient;

    public RealObsAdapter(ObsClient obsClient) {
        this.obsClient = obsClient;
    }

    @Override
    public int putObject(String bucketName, String objectKey, ByteBuffer payload) {
        try {
            PutObjectRequest request = new PutObjectRequest();
            request.setBucketName(bucketName);
            request.setObjectKey(objectKey);
            
            // 【架构师优化】：通过自定义流，将 Direct ByteBuffer 直接传给 SDK，避免 byte[] 拷贝
            if (payload != null) {
                request.setInput(new ByteBufferInputStream(payload));
            }

            PutObjectResult result = obsClient.putObject(request);
            return result.getStatusCode();

        } catch (ObsException e) {
            // 参考 SDK 开发指南，捕获服务端返回的 HTTP 状态码
            return e.getResponseCode();
        } catch (Exception e) {
            // 客户端本地异常（如网络断开、DNS 解析失败等）返回 0
            return 0;
        }
    }

    @Override
    public int getObject(String bucketName, String objectKey, String range) {
        try {
            GetObjectRequest request = new GetObjectRequest(bucketName, objectKey);
            if (range != null && !range.isBlank()) {
                request.setRangeStart(parseRangeStart(range));
                request.setRangeEnd(parseRangeEnd(range));
            }

            ObsObject obsObject = obsClient.getObject(request);
            
            // 【关键】：必须彻底消耗并关闭 InputStream，否则连接池资源会耗尽
            consumeAndCloseStream(obsObject.getObjectContent());
            
            return 200;

        } catch (ObsException e) {
            return e.getResponseCode();
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    public int deleteObject(String bucketName, String objectKey) {
        try {
            DeleteObjectResult result = obsClient.deleteObject(bucketName, objectKey);
            return result.getStatusCode();
        } catch (ObsException e) {
            return e.getResponseCode();
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    public int multipartUpload(String bucketName, String objectKey, ByteBuffer payload, int partCount, long partSize) {
        try {
            // 1. 初始化分段
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, objectKey);
            InitiateMultipartUploadResult initResult = obsClient.initiateMultipartUpload(initRequest);
            String uploadId = initResult.getUploadId();

            // 2. 循环上传段 (压测简化版：此处可根据需要引入线程内并发)
            java.util.List<PartEtag> etags = new java.util.ArrayList<>();
            for (int i = 1; i <= partCount; i++) {
                UploadPartRequest partRequest = new UploadPartRequest();
                partRequest.setBucketName(bucketName);
                partRequest.setObjectKey(objectKey);
                partRequest.setUploadId(uploadId);
                partRequest.setPartNumber(i);
                
                // 模拟切片：重置 position 并读取固定长度
                payload.position((int) ((i - 1) * partSize));
                partRequest.setInput(new ByteBufferInputStream(payload));
                partRequest.setPartSize(partSize);
                
                UploadPartResult partResult = obsClient.uploadPart(partRequest);
                etags.add(new PartEtag(partResult.getEtag(), partResult.getPartNumber()));
            }

            // 3. 合并分段
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(bucketName, objectKey, uploadId, etags);
            CompleteMultipartUploadResult result = obsClient.completeMultipartUpload(completeRequest);
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
            // 使用 SDK 封装的高阶断点续传接口
            UploadFileRequest request = new UploadFileRequest(bucketName, objectKey);
            request.setUploadFile(filePath);
            request.setTaskNum(taskNum);
            request.setPartSize(partSize);
            request.setEnableCheckpoint(enableCheckpoint);

            CompleteMultipartUploadResult result = obsClient.uploadFile(request);
            return result.getStatusCode();
        } catch (ObsException e) {
            return e.getResponseCode();
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * 辅助方法：快速排空输入流并归还连接池
     */
    private void consumeAndCloseStream(InputStream in) {
        if (in == null) return;
        try (in) {
            byte[] drain = new byte[8192];
            while (in.read(drain) != -1) {
                // 如果需要校验数据一致性，在此扩展比对逻辑
            }
        } catch (Exception ignore) {}
    }

    // 简单的 Range 解析逻辑 (示例: "0-1024")
    private long parseRangeStart(String range) {
        return Long.parseLong(range.split("-")[0]);
    }
    private long parseRangeEnd(String range) {
        String[] parts = range.split("-");
        return parts.length > 1 ? Long.parseLong(parts[1]) : -1L;
    }
}
