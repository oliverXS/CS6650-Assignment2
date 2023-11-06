import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * @author xiaorui
 */
@Slf4j
public class CSVUtility {
   private BufferedWriter csvWriter;

    public void initializeCSVWriter(String filename) {
        try {
            if (!filename.endsWith(".csv")) {
                filename += ".csv";
            }
            csvWriter = Files.newBufferedWriter(Paths.get(filename), StandardOpenOption.CREATE);
            // write the header
            csvWriter.write("Start Time,Request Type,Latency,Response Code\n");
            log.info("Initialize CSV Writer!");
        } catch (IOException e) {
            log.error("Failed to initialize CSV writer", e);
            System.exit(1);
        }
    }

    public synchronized void writeRecord(ApiRecord record) {
        try {
            csvWriter.write(String.format("%d,%s,%d,%d\n",
                    record.getStartTime(),
                    record.getRequestType(),
                    record.getLatency(),
                    record.getResponseCode()));
        } catch (IOException e) {
            log.error("Failed to write record to CSV", e);
        }
    }

    public void closeCSVWriter() {
        try {
            if (csvWriter != null) {
                csvWriter.close();
                log.info("Close CSV Writer!");
            }
        } catch (IOException e) {
            log.error("Failed to close CSV writer", e);
        }
    }

}
