import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xiaorui
 */
@Slf4j
public class ClientTest {
    private static String ipAddr;
    private static final AtomicInteger SUCCESSFUL_REQUESTS = new AtomicInteger(0);
    private static final AtomicInteger FAILED_REQUESTS = new AtomicInteger(0);
    private static final ConcurrentLinkedDeque<Long> getLatencies = new ConcurrentLinkedDeque<>();
    private static final ConcurrentLinkedDeque<Long> postLatencies = new ConcurrentLinkedDeque<>();
    private static final CSVUtility csvUtility = new CSVUtility();
    private static ApiHttpClient apiClient;
    private static final ThreadUtility threadUtility = new ThreadUtility();
    private static final Gson gson = new Gson();

    public static void main(String[] args) throws InterruptedException {
        // Check arguments
        if (args.length < Constant.ARGS_NUM) {
            log.error("Usage: java ApiClient <threadGroupSize> <numThreadGroups> <delay> <IPAddr>");
            System.exit(1);
        }

        // Initialize arguments
        int threadGroupSize = Integer.parseInt(args[0]);
        int numThreadGroups = Integer.parseInt(args[1]);
        int delay = Integer.parseInt(args[2]);
        ipAddr = args[3];
        apiClient = new ApiHttpClient(ipAddr);

        // Initial Phase
        ThreadPoolExecutor initialExecutor = threadUtility.generateInitialExecutor(Constant.INITIAL_THREADS);
        log.info("Start Initial Phase!");
        for (int i = 0; i < Constant.INITIAL_THREADS; i++) {
            initialExecutor.submit(() -> {
                HttpPost postRequest = apiClient.createPostRequest();
                HttpGet getRequest;
                for (int j = 0; j < Constant.INITIAL_API_CALLS; j++) {
                    // Call POST
                    try (CloseableHttpResponse response = apiClient.executeRequest(postRequest)) {
                        String responseBody = EntityUtils.toString(response.getEntity());
                        PostResponse postResponse = gson.fromJson(responseBody, PostResponse.class);
                        String albumId = postResponse.getAlbumId();
                        getRequest = apiClient.createGetRequest(albumId);
                        EntityUtils.consume(response.getEntity());
                    } catch (Exception e) {
                        log.error("Exception occurred in initial phase: POST");
                        throw new RuntimeException(e);
                    }
                    // Call GET
                    try (CloseableHttpResponse response = apiClient.executeRequest(getRequest)) {
                        EntityUtils.consume(response.getEntity());
                    } catch (Exception e) {
                        log.error("Exception occurred in initial phase: GET");
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        initialExecutor.shutdown();
        try {
            initialExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Main thread interrupted while waiting for task completion", e);
        }
        log.info("End Initial Phase!");

        // Loop phase
        ThreadPoolExecutor loopExecutor = threadUtility.generateLoopExecutor(threadGroupSize, numThreadGroups);
        log.info("Start Loop Phase!");
        csvUtility.initializeCSVWriter("Three_Servers_" + threadGroupSize + "_" + numThreadGroups);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numThreadGroups; i++) {
            for (int j = 0; j < threadGroupSize; j++) {
                loopExecutor.submit(() -> {
                    HttpPost postRequest = apiClient.createPostRequest();
                    for (int k = 0; k < Constant.LOOP_API_CALLS; k++) {
                        String albumId = executePost(postRequest);
                        HttpGet getRequest = apiClient.createGetRequest(albumId);
                        executeGet(getRequest);
                    }
                });
            }
            if (i < numThreadGroups - 1) {
                Thread.sleep(delay * 1000L);
            }
        }
        loopExecutor.shutdown();
        try {
            loopExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Main thread interrupted while waiting for task completion", e);
        }
        log.info("End Loop Phase!");
        long endTime = System.currentTimeMillis();
        csvUtility.closeCSVWriter();
        System.out.println("\n");

        // Time in s
        long totalTime = endTime - startTime;
        // Time in ms
        double wallTime = totalTime / 1000.0;
        log.info("Wall Time: " + wallTime + " seconds");
        log.info("Successful Requests: " + SUCCESSFUL_REQUESTS.get() + " times");
        log.info("Failed Requests: " + FAILED_REQUESTS.get() + " times");
        double throughput = (SUCCESSFUL_REQUESTS.get() + FAILED_REQUESTS.get()) / wallTime;
        log.info("Throughput: " + throughput + " request/second");
        System.out.println("\n");

        List<Long> combinedGetLatencies = new ArrayList<>(getLatencies);
        List<Long> combinedPostLatencies = new ArrayList<>(postLatencies);
        computeLatencyStats(combinedPostLatencies, "POST");
        System.out.println("\n");
        computeLatencyStats(combinedGetLatencies, "GET");
    }

    private static String executePost(HttpUriRequest request) {
        int retries = Constant.MAX_RETRIES;

        while (retries > 0) {
            long startTime = System.currentTimeMillis();
            try (CloseableHttpResponse response = apiClient.executeRequest(request)) {
                String responseBody = EntityUtils.toString(response.getEntity());
                PostResponse postResponse = gson.fromJson(responseBody, PostResponse.class);
                String albumId = postResponse.getAlbumId();
                EntityUtils.consume(response.getEntity());

                long endTime = System.currentTimeMillis();
                long latency = endTime - startTime;
                postLatencies.add(latency);

                int statusCode = response.getStatusLine().getStatusCode();
                ApiRecord apiRecord = new ApiRecord(startTime, request.getMethod(), latency, statusCode);
                csvUtility.writeRecord(apiRecord);

                if (statusCode >= 200 && statusCode < 300) {
                    SUCCESSFUL_REQUESTS.incrementAndGet();
                    return albumId;
                } else if (statusCode >= 400 && statusCode < 600) {
                    FAILED_REQUESTS.incrementAndGet();
                    retries--;
                    if (retries == 0) {
                        log.error("Failed to execute request after 5 retries. URL: " + request.getURI() + request.getMethod());
                    }
                }
            } catch (Exception e) {
                FAILED_REQUESTS.incrementAndGet();
                e.printStackTrace();
                log.info("Exception in Loop phase: " + request.getMethod());
            }
        }
        return null;
    }

    private static void executeGet(HttpUriRequest request) {
        int retries = Constant.MAX_RETRIES;

        while (retries > 0) {
            long startTime = System.currentTimeMillis();
            try (CloseableHttpResponse response = apiClient.executeRequest(request)) {
                EntityUtils.consume(response.getEntity());
                long endTime = System.currentTimeMillis();
                long latency = endTime - startTime;
                getLatencies.add(latency);

                int statusCode = response.getStatusLine().getStatusCode();
                ApiRecord apiRecord = new ApiRecord(startTime, request.getMethod(), latency, statusCode);
                csvUtility.writeRecord(apiRecord);

                if (statusCode >= 200 && statusCode < 300) {
                    SUCCESSFUL_REQUESTS.incrementAndGet();
                    return;
                } else if (statusCode >= 400 && statusCode < 600) {
                    FAILED_REQUESTS.incrementAndGet();
                    retries--;
                    if (retries == 0) {
                        log.error("Failed to execute request after 5 retries. URL: " + request.getURI() + request.getMethod());
                    }
                }
            } catch (Exception e) {
                FAILED_REQUESTS.incrementAndGet();
                e.printStackTrace();
                log.info("Exception in Loop phase: " + request.getMethod());
            }
        }
    }

    private static void computeLatencyStats(List<Long> latencies, String methodType) {
        Collections.sort(latencies);
        int size = latencies.size();
        long sum = 0;
        for (long latency : latencies) {
            sum += latency;
        }
        double mean = sum / (double) size;
        double median = size % 2 == 0 ? (latencies.get(size / 2 - 1) + latencies.get(size / 2)) / 2.0 : latencies.get(size / 2);
        double p99 = latencies.get((int) (size * 0.99));
        long min = latencies.get(0);
        long max = latencies.get(size - 1);

        log.info(methodType + " Mean Latency: " + mean + "ms");
        log.info(methodType + " Median Latency: " + median + "ms");
        log.info(methodType + " 99th Percentile Latency: " + p99 + "ms");
        log.info(methodType + " Min Latency: " + min + "ms");
        log.info(methodType + " Max Latency: " + max + "ms");
    }
}
