import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author xiaorui
 */
@Slf4j
public class ApiHttpClient {
    private CloseableHttpClient httpClient;
    private String ipAddr;
    private final Gson gson = new Gson();
    private final byte[] imageBytes = loadImageData();
    private final String PROFILE_JSON = gson.toJson(new Profile("Sex Pistols", "Never Mind The Bollocks!", "1977"));

    public ApiHttpClient(String ipAddr) {
        this.ipAddr = ipAddr;
        this.httpClient = generateHttpClient();
    }

    private CloseableHttpClient generateHttpClient() {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(3000);
        connectionManager.setDefaultMaxPerRoute(3000);
        return HttpClients.custom().setConnectionManager(connectionManager).build();
    }

    public HttpPost createPostRequest() {
        URI postUri = URI.create(ipAddr + "/albums");
        HttpPost postRequest = new HttpPost(postUri);

        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addBinaryBody("image", imageBytes, ContentType.IMAGE_PNG, "image.png");
        builder.addTextBody("profile", PROFILE_JSON, ContentType.APPLICATION_JSON);
        HttpEntity entity = builder.build();
        postRequest.setEntity(entity);

        return postRequest;
    }

    public HttpGet createGetRequest(String albumId) {
        URI getUri = URI.create(ipAddr + "/albums/" + albumId);
        return new HttpGet(getUri);
    }

    public CloseableHttpResponse executeRequest(HttpUriRequest request) throws Exception {
        return httpClient.execute(request);
    }

    private byte[] loadImageData() {
        byte[] imageBytes = null;
        try {
            Path imagePath = Paths.get("src/main/resources/nmtb.png");
            imageBytes = Files.readAllBytes(imagePath);
        } catch (IOException e) {
            log.error("Failed to read the image file into memory", e);
            System.exit(1);
        }
        return imageBytes;
    }
}
