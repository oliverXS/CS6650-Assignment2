import lombok.*;

/**
 * @author xiaorui
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ApiRecord {
    private long startTime;
    private String requestType;
    private long latency;
    private int responseCode;
}
