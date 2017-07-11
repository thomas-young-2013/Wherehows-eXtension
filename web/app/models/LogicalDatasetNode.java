package models;

/**
 * Created by thomas young on 7/5/17.
 */
public class LogicalDatasetNode {
    public Long id;
    public String name;
    public String path;
    public String children;
    public Long datasetId;

    public LogicalDatasetNode() {}

    public LogicalDatasetNode(Long id, String path) {
        this.id = id;
        this.path = path;
    }

    public LogicalDatasetNode(Long id, String path, String children) {
        this.id = id;
        this.path = path;
        this.children = children;
    }

    public LogicalDatasetNode(Long id, String path, Long datasetId) {
        this.id = id;
        this.path = path;
        this.datasetId = datasetId;
    }
}
