import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Ye Tianyi
 * @version 1.0
 * @date 2024/2/29 17:24
 */
public class SQLInsert {

    private static final String FILE_PATH = "C:\\Users\\Admin\\Desktop\\数据2.sql";
    private static final String URL = "http://10.192.18.226:30080/api/datasync_outdoor/execute_sql_file";

    public static void main(String[] args) throws IOException, InterruptedException {

        System.setProperty( "org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog" );
        int lineCount = readLineCount(FILE_PATH); // 获取总行数
        int batchSize = 250;


        BufferedReader br = new BufferedReader(new FileReader(FILE_PATH));

        for(int i=0; i<lineCount; i+=batchSize) {
            System.out.println("当前处理从"+i+"到第"+(i+batchSize)+"行");
            List<String> batch = new ArrayList<>();

            for(int j=0; j<batchSize && i+j<lineCount; j++) {
                batch.add(br.readLine());
            }

            File tempFile = writeBatchToTempFile(batch);

            String res = post(tempFile);
            if (res.contains("504 Gateway") || res.contains("The upstream server is timing out")) {
                again(tempFile,"从"+i+"到第"+(i+batchSize)+"行");
            } else if (res.contains("An invalid response was received from the upstream server")) {
                // 一行行补
                oneLineInsert(tempFile,"从"+i+"到第"+(i+batchSize)+"行");
            }
            tempFile.delete(); // 删除临时文件
        }

        if(lineCount%batchSize > 0) {
            List<String> remainBatch = new ArrayList<>();
            for(int j=0; j<lineCount%batchSize; j++) {
                remainBatch.add(br.readLine());
            }

            File tempFile = writeBatchToTempFile(remainBatch);

            String res = post(tempFile);
            if (res.contains("504 Gateway") || res.contains("The upstream server is timing out")) {
                again(tempFile,"执行最后一次");
            } else if (res.contains("An invalid response was received from the upstream server")) {
                // 一行行补
                oneLineInsert(tempFile,"执行最后一次");
            }
            tempFile.delete(); // 删除临时文件
        }

        br.close();
    }

    /**
     * 一行行执行
     * */
    private static void oneLineInsert(File tempFile, String s) throws IOException, InterruptedException {
        BufferedReader br = new BufferedReader(new FileReader(tempFile.getAbsolutePath()));
        String line;
        while((line = br.readLine()) != null) {
            List<String> lineBatch = new ArrayList<>();
            lineBatch.add(line);
            File tempLine = writeBatchToTempFile(lineBatch);
            System.out.println("=====================================================补"+s);
            String res = post(tempLine);
            if (res.contains("504 Gateway") || res.contains("The upstream server is timing out") ) {
                System.out.println("再次尝试单行补充插入");
                onelineagain(tempLine);
            } else if (res.contains("An invalid response was received from the upstream server")) {
                System.out.println("再次尝试单行补充插入");
                Thread.sleep(5000);
                onelineagain(tempLine);
            }
            tempLine.delete();
        }

        br.close();
    }

    /**
     * 一行行补的时候出现网络波动再次请求
     * */
    private static void onelineagain(File tempLine) {
        String post = post(tempLine);
        if (post.contains("nternal Server Error") || post.contains("ok")) {
            return;
        } else if (post.contains("504 Gateway") || post.contains("The upstream server is timing out") || post.contains("An invalid response was received from the upstream server")) {
            onelineagain(tempLine);
        }
    }

    /**
     * 单次请求重发
     * */
    private static void again(File tempFile,String s) throws IOException, InterruptedException {
        String post = post(tempFile);
        if (post.contains("nternal Server Error") || post.contains("ok")) {
            return;
        } else if (post.contains("504 Gateway") || post.contains("The upstream server is timing out")) {
            again(tempFile,s);
        } else if (post.contains("An invalid response was received from the upstream server")) {
            // 一行行试
            oneLineInsert(tempFile,s);
        }
    }


    // 读取行数
    public static int readLineCount(String filePath) throws IOException {

        int count = 0;

        BufferedReader br = new BufferedReader(new FileReader(filePath));

        while(br.readLine() != null) {
            count++;
        }

        br.close();

        return count;

    }


    // 写入临时文件
    public static File writeBatchToTempFile(List<String> batch) throws IOException {
        File tempFile = File.createTempFile("sql", ".sql");
        BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile));
        bw.write(String.join("\n", batch));
        bw.close();
        return tempFile;
    }

    private static void readFileContent(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));

        String line;
        while((line = br.readLine()) != null) {
            System.out.println(line);
        }

        br.close();
    }

    private static String post(File file) {

        String result = "";
        HttpClientBuilder clientBuilder = HttpClientBuilder.create();
        clientBuilder.disableCookieManagement()
                .disableRedirectHandling()
                .disableAuthCaching();

        // 创建HttpClient对象
        CloseableHttpClient httpClient = clientBuilder.build();

        // 创建HttpPost请求
        HttpPost httpPost = new HttpPost(URL);

        // 设置请求体参数
        // 创建MultipartEntityBuilder，并添加参数
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addBinaryBody("sql_file", file, ContentType.APPLICATION_OCTET_STREAM, file.getName());
        HttpEntity entity = builder.build();
        httpPost.setEntity(entity);
        // 计算Content-Length并设置请求头
        try {
            //httpPost.setHeader("Content-Length", String.valueOf(1900));
            //httpPost.setHeader("Content-Type", "multipart/form-data; boundary=----WebKitFormBoundaryDCfDFBAzpV6AOO7N");
            httpPost.setHeader("Cookie", "timestamp=1709285981708");
            httpPost.setHeader("Authorization", "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJkeVU3aWhOUHBnTWJ4eVJCR1d2cG0zVDNZTGRXSWt1NyIsImV4cCI6MTcxMTQwMjk3NCwibmJmIjoxNzExMzMwOTc0LCJpYXQiOjE3MTEzMzA5NzR9.GkIhemGe5xDe0ugV0TbdJxCSravuhBY0-5VUUflyVNA");
            httpPost.setHeader("Connection", "keep-alive");
            httpPost.setHeader("Accept-Encoding", "gzip, deflate, br");
            httpPost.setHeader("Accept", "*/*");
            httpPost.setHeader("Host", "10.192.18.226:30080");
            httpPost.setHeader("Origin", "http://10.192.18.226:30080");
            httpPost.setHeader("User-Agent", "PostmanRuntime/7.36.3");
            httpPost.setHeader("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6");
            httpPost.setHeader("Cache-Control", "max-age=0");
            httpPost.setHeader("Referer", "http://10.192.18.226:30080/frontend/");
            httpPost.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0");

            System.setProperty("org.apache.http.wire.logging.enabled", "false");
            // 发送请求并获取响应
            HttpResponse response = httpClient.execute(httpPost);

            // 处理响应
            HttpEntity responseEntity = response.getEntity();
            if (responseEntity != null) {
                String responseStr = EntityUtils.toString(responseEntity);
                System.out.println("Response: " + responseStr);
                result = responseStr;
                // 在这里处理返回的内容
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭HttpClient连接
            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return result;
    }
}
