package connect.main;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;

import java.io.IOException;

public class Connect {
    private static Connect instance = new Connect();
    private RestHighLevelClient client;

    public boolean connectSession() throws IOException {

        HttpHost host = new HttpHost("localhost", 9200, "http");

        client = new RestHighLevelClient(RestClient.builder(host));

        client.indices().refresh(new RefreshRequest(), RequestOptions.DEFAULT);

        ClusterHealthResponse health = client.cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);

        return !health.getStatus().equals(ClusterHealthStatus.RED);
    }

    public static Connect getInstance() {
        return instance;
    }

    public RestHighLevelClient getClient() {
        return client;
    }
}
