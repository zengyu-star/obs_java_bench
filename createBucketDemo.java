import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.AccessControlList;
import com.obs.services.model.AvailableZoneEnum;
import com.obs.services.model.CreateBucketRequest;
import com.obs.services.model.ObsBucket;
import com.obs.services.model.StorageClassEnum;

public class CreateBucket001 {
    public static void main(String[] args) {
        // 您可以通过环境变量获取访问密钥AK/SK，也可以使用其他外部引入方式传入。如果使用硬编码可能会存在泄露风险。
        // 您可以登录访问管理控制台获取访问密钥AK/SK
        String ak = System.getenv("ACCESS_KEY_ID");
        String sk = System.getenv("SECRET_ACCESS_KEY_ID");
        // 【可选】如果使用临时AK/SK和SecurityToken访问OBS，同样建议您尽量避免使用硬编码，以降低信息泄露风险。
        // 您可以通过环境变量获取访问密钥AK/SK/SecurityToken，也可以使用其他外部引入方式传入。
        // String securityToken = System.getenv("SECURITY_TOKEN");

        // endpoint填写桶所在的endpoint, 此处以华北-北京四为例，其他地区请按实际情况填写。
        String endPoint = "https://obs.cn-north-4.myhuaweicloud.com";

        // 您可以通过环境变量获取endPoint，也可以使用其他外部引入方式传入。
        //String endPoint = System.getenv("ENDPOINT");

        // 创建ObsClient实例
        // 使用永久AK/SK初始化客户端
        ObsClient obsClient = new ObsClient(ak, sk,endPoint);
        // 使用临时AK/SK和SecurityToken初始化客户端
        // ObsClient obsClient = new ObsClient(ak, sk, securityToken, endPoint);
        
        try {
            CreateBucketRequest request = new CreateBucketRequest();
            //示例桶名
            String exampleBucket = "examplebucket";
            //示例桶区域位置
            String exampleLocation = "cn-north-4";
            request.setBucketName(exampleBucket);
            // 设置桶ACL为私有读写，默认也是私有读写
            request.setAcl(AccessControlList.REST_CANNED_PRIVATE);
            // 设置桶的存储类别为标准存储
            request.setBucketStorageClass(StorageClassEnum.STANDARD);
            // 设置桶区域位置(以区域为华北-北京四为例)，location 需要与 endpoint的位置信息一致
            request.setLocation(exampleLocation);
            // 指定创建多AZ桶，如果不设置，默认创建单AZ桶
            request.setAvailableZone(AvailableZoneEnum.MULTI_AZ);
            // 创建桶
            ObsBucket bucket = obsClient.createBucket(request);
            // 创建桶成功
            System.out.println("CreateBucket successfully");
            System.out.println("RequestId:"+bucket.getRequestId());


        } catch (ObsException e) {
            System.out.println("CreateBucket failed");
            // 请求失败,打印http状态码
            System.out.println("HTTP Code: " + e.getResponseCode());
            // 请求失败,打印服务端错误码
            System.out.println("Error Code:" + e.getErrorCode());
            // 请求失败,打印详细错误信息
            System.out.println("Error Message: " + e.getErrorMessage());
            // 请求失败,打印请求id
            System.out.println("Request ID:" + e.getErrorRequestId());
            System.out.println("Host ID:" + e.getErrorHostId());
        } catch (Exception e) {
            System.out.println("CreateBucket failed");
            // 其他异常信息打印
            e.printStackTrace();

        }
    }
}
