package connect.main.rest.worker;

import connect.main.Utils;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.Message;
import rx.functions.Action1;

import java.lang.reflect.Field;
import java.util.Map;

public class UserWorkerVerticle extends AbstractVerticle {
    private final String CATEGORY = this.getClass().getSimpleName().replace("WorkerVerticle", "").toLowerCase();
    @Override
    public void start() {
        // 필드명 으로 consumer 등록
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

            String key = Utils.create("db", sm_type, name.toLowerCase(), jsonObject.getMap());

            message.reply(key);
        } catch (Exception e) {
            System.out.println("Error : " + e.getMessage());
            message.fail(500, e.getMessage());
        }
    };
}
