package connect.main;

import io.vertx.core.json.JsonObject;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

public class Utils {
    private static RestHighLevelClient client = Connect.getInstance().getClient();

    static public String create(String index, String sm_type, String key, Map<String, Object> indexMap) throws IOException {
        IndexRequest indexRequest = new IndexRequest()
                .index(index)
                .source(new JsonObject(indexMap).toString(), XContentType.JSON);
        if (key != null) indexRequest.id(sm_type + key);

        client.index(indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);

        return key;
    }

    public static GetResponse get(String index, String type, String key) throws ElasticsearchStatusException, IOException {
        GetRequest getRequest = new GetRequest(index, type + key);
        return client.get(getRequest, RequestOptions.DEFAULT);
    }

    public static String delete(String index, String sm_type, String key) throws IOException {
        DeleteRequest deleteRequest = (new DeleteRequest()).index(index).type(index).id(sm_type + key);
        client.delete(deleteRequest, RequestOptions.DEFAULT);
        return key;
    }

    static public String update(String index, String sm_type, String key, Map<String, Object> updateMap) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest()
                .index(index)
                .type(index)
                .id(sm_type + key)
                .doc(new JsonObject(updateMap).toString(), XContentType.JSON)
                .docAsUpsert(true)
                .retryOnConflict(5);

        client.update(updateRequest, RequestOptions.DEFAULT);
        return key;
    }
}
