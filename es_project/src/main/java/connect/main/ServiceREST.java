package connect.main;

import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.reflections.Reflections;
import rx.functions.Func2;

import java.lang.reflect.Field;
import java.util.Set;

public class ServiceREST extends AbstractVerticle {
    private String ROOT_URI = "/service/";
    private final String HANDLER = "Handler";

    @Override
    public void start(Future<Void> fut) {
        try {
            Router router = Router.router(vertx);
            // API
            initRouter(router);

            vertx.createHttpServer().requestHandler(router::accept)
                    .listen(config().getInteger("al", 8083), result -> {
                        if (result.succeeded()) {
                            System.out.println("REST Engine is online = " + "localhost" + ":" + config().getInteger("al", 8083));
                            fut.complete();
                        } else {
                            System.out.println("REST Engine is online can not start !");
                            fut.fail(result.cause());
                        }
                    });
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private void initRouter(Router router) {
        Reflections reflections = new Reflections("connect.main.rest.worker");
        // AbstractVerticle 을 상속받은 클래스 Set을 만든다.
        Set<Class<? extends io.vertx.rxjava.core.AbstractVerticle>> classes = reflections.getSubTypesOf(io.vertx.rxjava.core.AbstractVerticle.class);

        for (Class<? extends io.vertx.rxjava.core.AbstractVerticle> aClass : classes) {
            String category = aClass.getSimpleName().replace("WorkerVerticle", "").toLowerCase();
            Field[] f = aClass.getDeclaredFields();
            for (Field field : f) {
                if (!field.getType().getSimpleName().equals("Action1")) continue;
                String function = field.getName().replace(category, "").replace(HANDLER, "");

                /**
                 * 필드이름에 들어간 단어에 따라서 REST METHOD 변경
                 * update & replace & edit    = PUT
                 * add & request & patch      = POST
                 * delete                     = DELETE
                 * default method             = GET
                 *
                 * Body 부분을 parsing
                 */

                // route URI
                String uri = ROOT_URI + category + "/" + function;
                String fieldName = field.getName();

                if (fieldName.contains("update")
                        || fieldName.contains("replace")
                        || fieldName.contains("edit")) { // PUT
                    router.put(uri).handler(this::putHandler);
                    System.out.println("PUT    uri : " + uri);
                } else if (fieldName.contains("add")
                        || fieldName.contains("patch")
                        || fieldName.contains("request")) { // POST
                    router.post(uri).handler(this::postHandler);
                    System.out.println("POST   uri : " + uri);
                } else if (fieldName.contains("delete")
                        || fieldName.contains("remove")
                        || fieldName.contains("del")) { // DELETE
                    router.delete(uri).handler(this::deleteHandler);
                    System.out.println("DELETE uri : " + uri);
                } else { // GET
                    router.get(uri).handler(this::getHandler);
                    System.out.println("GET    uri : " + uri);
                }
            }
            System.out.println("Complete Add Routing URIs" + category);
        }
    }

    private void getHandler(RoutingContext routingContext) {
        HttpServerResponse response = routingContext.response();
        HttpServerRequest request = routingContext.request();

        String[] temp = request.path().split("/");

        String category = temp[2];
        String func = temp[3];
        MultiMap params = request.params();

        JsonObject jsonObject = new JsonObject();
        jsonObject.put("category", category);
        jsonObject.put("func", func);

        JsonObject tempJson = new JsonObject();
        for (String name : params.names()) tempJson.put(name, params.get(name));
        jsonObject.put("body", tempJson);

        System.out.println("Send Message : GET  " + ROOT_URI + category + "/" + func + ",  " + jsonObject.toString());

        vertx.eventBus().send(category + func + HANDLER, jsonObject, reply -> {
            if (reply.failed()) {
                sendError(((ReplyException) reply.cause()).failureCode(), response, reply.cause().getMessage());
            } else {
                if (reply.result().headers().get("wait") != null) {
                    vertx.setPeriodic(3000, isup.call(response, reply));
                } else {
                    String content = reply.result().body().toString();
                    sendOk(response, content);
                }
            }
        });
    }

    private Func2<HttpServerResponse, AsyncResult<Message<Object>>, Handler<Long>> isup = (response, asyncResult) -> id ->
            vertx.eventBus().send(asyncResult.result().headers().get("wait"), null, reply -> {
                if (reply.succeeded()) {
                    vertx.cancelTimer(id);
                    String content = asyncResult.result().body().toString();
                    sendOk(response, content);
                } else {
                    sendOk(response, "false");
                }
            });

    private void deleteHandler(RoutingContext routingContext) {
        HttpServerResponse response = routingContext.response();
        HttpServerRequest request = routingContext.request();

        String[] temp = request.path().split("/");

        String category = temp[2];
        String func = temp[3];
        MultiMap params = request.params();
        System.out.println("params to json : " + Json.encode(params));

        JsonObject jsonObject = new JsonObject();
        jsonObject.put("category", category);
        jsonObject.put("func", func);

        JsonObject tempJson = new JsonObject();
        for (String name : params.names()) tempJson.put(name, params.get(name));

        if (params.size() == 0) {
            request.bodyHandler(buffer -> {
                try {
                    jsonObject.put("body", new JsonObject(buffer.toString()));
                } catch (Exception e1) {
                    try {
                        jsonObject.put("body", new JsonArray(buffer.toString()));
                    } catch (Exception e) {
                        sendError(400, response, "unknown request body");
                        return;
                    }
                }
            });
        }

        jsonObject.put("body", tempJson);

        System.out.println("Send Message : DELETE  " + ROOT_URI + category + "/" + func + ",  " + jsonObject.toString());

        vertx.eventBus().send(category + func + HANDLER, jsonObject, reply -> {
            if (reply.failed()) {
                sendError(((ReplyException) reply.cause()).failureCode(), response, reply.cause().getMessage());
            } else {
                String content = reply.result().body().toString();
                sendOk(response, content);
            }
        });
    }

    private void postHandler(RoutingContext routingContext) {
//        long init = System.currentTimeMillis();
        HttpServerResponse response = routingContext.response();
        HttpServerRequest request = routingContext.request();

        String[] temp = request.path().split("/");

        String category = temp[2];
        String func = temp[3];

        request.bodyHandler(buffer -> {

            JsonObject jsonObject = new JsonObject();
            jsonObject.put("category", category);
            jsonObject.put("func", func);

            try {
                jsonObject.put("body", new JsonObject(buffer.toString()));
            } catch (Exception e1) {
                try {
                    jsonObject.put("body", new JsonArray(buffer.toString()));
                } catch (Exception e) {
                    sendError(400, response, "unknown request body");
                    return;
                }
            }
            System.out.println("Send Message : POST  " + ROOT_URI + category + "/" + func + ",  " + jsonObject.toString());

            vertx.eventBus().send(category + func + HANDLER, jsonObject, reply -> {
                if (reply.failed()) {
                    sendError(((ReplyException) reply.cause()).failureCode(), response, reply.cause().getMessage());
                } else {
                    String content = reply.result().body().toString();
                    sendOk(response.setStatusCode(201), content);
                }
            });
        });
    }

    private void putHandler(RoutingContext routingContext) {
        HttpServerResponse response = routingContext.response();
        HttpServerRequest request = routingContext.request();

        String[] temp = request.path().split("/");

        String category = temp[2];
        String func = temp[3];

        request.bodyHandler(buffer -> {
            JsonObject jsonObject = new JsonObject();
            jsonObject.put("category", category);
            jsonObject.put("func", func);
            try {
                jsonObject.put("body", new JsonObject(buffer.toString()));
            } catch (Exception e1) {
                try {
                    jsonObject.put("body", new JsonArray(buffer.toString()));
                } catch (Exception e) {
                    sendError(400, response, "unknown request body");
                    return;
                }
            }
            System.out.println("Send Message : PUT  " + ROOT_URI + category + "/" + func + ",  " + jsonObject.toString());

            vertx.eventBus().send(category + func + HANDLER, jsonObject, reply -> {
                if (reply.failed()) {
                    sendError(((ReplyException) reply.cause()).failureCode(), response, reply.cause().getMessage());
                } else {
                    String content = reply.result().body().toString();
                    sendOk(response, content);
                }
            });
        });
    }

    private void sendError(int statusCode, HttpServerResponse response, String message) {
        try {
            System.out.println("Send Error : " + message + ", statusCode : " + statusCode);
            response.setStatusCode(statusCode < 0 ? 500 : statusCode);
            response.setStatusMessage(message + "");
            response.end("{\"error\": \"" + message + "\"}");

        } catch (IllegalStateException e) {
            System.out.println("error: " + e.getMessage());
        }
    }

    private void sendOk(HttpServerResponse response, String content) {
        try {
            response.putHeader("content-type", "application/json").end(content);
        } catch (IllegalStateException e) {
            System.out.println("error: " + e.getMessage());
        }
    }

}
