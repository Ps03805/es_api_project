package connect.main.rest.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.javafx.binding.MapExpressionHelper;
import connect.main.Utils;
import connect.main.model.User;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.Message;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import rx.functions.Action1;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.Map;

import static connect.main.Utils.create;
import static connect.main.Utils.responseToJsonList;
import static connect.main.util.DateFormat.sdf_yyyyMMddHHmmss;

public class UserWorkerVerticle extends AbstractVerticle {
    private final String CATEGORY = this.getClass().getSimpleName().replace("WorkerVerticle", "").toLowerCase();
    @Override
    public void start() {
        for (Field field : this.getClass().getDeclaredFields()) {
            if (field.getType() == Action1.class) {
                System.out.println("Add Consumer " + CATEGORY + field.getName());
                try {
                    vertx.eventBus().consumer(CATEGORY + field.getName()).toObservable().subscribe((Action1) field.get(this));
                } catch (IllegalAccessException e) {
                    System.out.println("Add Consumer Error Occur " + CATEGORY + field.getName());
                    return;
                }
            }
        }
    }

    private final Action1<Message<JsonObject>> getHandler = message -> {
        try {
            JsonObject jsonObject = message.body()
                    .getJsonObject("body");
            String sm_type = message.body().getString("category");
            String name = jsonObject.getString("name");

            Map<String, Object> result = null;

            result = Utils.get("db", sm_type, name).getSourceAsMap();

            message.reply(new JsonObject(result).toString());
        } catch (Exception e) {
            System.out.println("Error : " + e.getMessage());
            message.fail(500, e.getMessage());
        }
    };

    private final Action1<Message<JsonObject>> addHandler = message -> {
        try {
            JsonObject jsonObject = message.body().getJsonObject("body");
            String sm_type = message.body().getString("category");
            String name = jsonObject.getString("name");
            String now = sdf_yyyyMMddHHmmss.format(new Date());
            jsonObject.put("created_time", now);
            jsonObject.put("updated_time", now);

            User user = new User(jsonObject);
            String key = Utils.create("db", sm_type, name.toLowerCase(), JsonObject.mapFrom(user).getMap());
            message.reply(key);
        } catch (Exception e) {
            System.out.println("Error : " + e.getMessage());
            message.fail(500, e.getMessage());
        }
    };
    private final Action1<Message<JsonObject>> listHandler = message -> {
        try {
            JsonObject jsonObject = message.body().getJsonObject("body");
            String sm_type = message.body().getString("category");

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.termQuery("sm_type", sm_type));

            SearchResponse response = Utils.search("db", sourceBuilder);

            message.reply(responseToJsonList(response).toString());
        } catch (Exception e) {
            System.out.println("Error : " + e.getMessage());
            message.fail(500, e.getMessage());
        }
    };
}
