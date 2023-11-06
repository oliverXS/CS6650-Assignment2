import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author xiaorui
 */
public class ThreadUtility {
    public ThreadPoolExecutor generateInitialExecutor(int initialThreads) {
        return new ThreadPoolExecutor(
                initialThreads,
                initialThreads + 2,
                1L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    public ThreadPoolExecutor generateLoopExecutor(int threadGroupSize, int numThreadGroups) {
        return new ThreadPoolExecutor(
                threadGroupSize * numThreadGroups,
                threadGroupSize * numThreadGroups + 2,
                1L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

}
