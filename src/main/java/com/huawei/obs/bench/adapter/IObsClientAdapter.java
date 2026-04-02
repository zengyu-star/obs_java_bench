package com.huawei.obs.bench.adapter;

import java.nio.ByteBuffer;

/**
 * OBS 客户端适配器接口 (Execution Plane - Adapter Interface)
 * 职责：定义压测所需的标准操作，屏蔽底层 SDK 差异。
 * 架构要求：返回值必须是 HTTP 状态码（int），以实现极速的状态统计。
 */
public interface IObsClientAdapter {

    /**
     * TestCase 201: 普通对象上传 (PUT Object)
     * @param bucketName 目标桶名
     * @param objectKey 对象键名
     * @param payload 待上传的数据流 (Direct ByteBuffer)
     * @return HTTP 状态码 (如 200, 403, 500)
     */
    int putObject(String bucketName, String objectKey, ByteBuffer payload);

    /**
     * TestCase 202: 普通对象下载 (GET Object)
     * @param bucketName 目标桶名
     * @param objectKey 对象键名
     * @param range 下载范围 (例如 "bytes=0-1024")，为 null 则执行全量下载
     * @return HTTP 状态码 (如 200, 206, 404)
     */
    int getObject(String bucketName, String objectKey, String range);

    /**
     * TestCase 204: 删除对象 (DELETE Object)
     * @param bucketName 目标桶名
     * @param objectKey 对象键名
     * @return HTTP 状态码 (如 204)
     */
    int deleteObject(String bucketName, String objectKey);

    /**
     * TestCase 216: 分段上传 (Multipart Upload)
     * 内部逻辑：Init -> UploadParts -> Complete
     * @param bucketName 目标桶名
     * @param objectKey 对象键名
     * @param payload 待上传的完整数据流
     * @param partCount 分段总数
     * @param partSize 每段大小
     * @return 最终状态码
     */
    int multipartUpload(String bucketName, String objectKey, ByteBuffer payload, int partCount, long partSize);

    /**
     * TestCase 230: 断点续传上传 (Resumable Upload)
     * 利用 SDK 封装的高阶接口进行大文件并发上传
     * @param bucketName 目标桶名
     * @param objectKey 对象键名
     * @param filePath 本地文件路径
     * @param taskNum 并发线程数
     * @param partSize 分段大小
     * @param enableCheckpoint 是否开启断点续传
     * @return 最终状态码
     */
    int resumableUpload(String bucketName, String objectKey, String filePath, int taskNum, long partSize, boolean enableCheckpoint);
}
