
package kafkacurrentassignment;

import java.util.List;
import javax.annotation.Generated;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

@Generated("jsonschema2pojo")
public class Partition {

    @SerializedName("topic")
    @Expose
    private String topic;
    @SerializedName("partition")
    @Expose
    private Integer partition;
    @SerializedName("replicas")
    @Expose
    private List<Integer> replicas;
    @SerializedName("log_dirs")
    @Expose
    private List<String> logDirs;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public List<Integer> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<Integer> replicas) {
        this.replicas = replicas;
    }

    public List<String> getLogDirs() {
        return logDirs;
    }

    public void setLogDirs(List<String> logDirs) {
        this.logDirs = logDirs;
    }

}
