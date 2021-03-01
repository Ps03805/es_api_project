package connect.main;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.reflections.Reflections;

import java.io.IOException;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class ElasticConnection extends AbstractVerticle {

    @Override
    public void start() throws IOException {
        do {
            boolean connect = Connect.getInstance().connectSession();
            if (!connect) {
                System.out.println("Couldn't Connect ElasticSearch");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ignore) {
                }
            } else {
                System.out.println("Elasticsearch Connected");
                break;
            }
        } while (true);

        Handler<AsyncResult<Void>> closeVertx = _void -> {
            if (_void.succeeded())
                System.out.println("Vertx Closing Succeed");
            else
                System.out.println("Vertx Closing Failed");
        };

        Supplier<DeploymentOptions> optsSupplier = () -> new DeploymentOptions();

        BiFunction<Handler<AsyncResult<Void>>, String, Handler<AsyncResult<String>>> supplyDeployHandle = (handle, str) ->
                result -> {
                    System.out.println("[ " + str + " ]" + " is deployed with ID : " + result.result());
                    if (!result.succeeded())
                        vertx.close(handle);
                };

        Reflections reflections = new Reflections("connect.main.rest.worker");
        Set<Class<? extends io.vertx.rxjava.core.AbstractVerticle>> classes = reflections.getSubTypesOf(io.vertx.rxjava.core.AbstractVerticle.class);
        for (Class<? extends io.vertx.rxjava.core.AbstractVerticle> aClass : classes) {
            vertx.deployVerticle(
                    aClass.getName(),
                    optsSupplier.get().setWorker(true),
                    supplyDeployHandle.apply(closeVertx, aClass.getSimpleName())
            );
        }

        vertx.deployVerticle(
                ServiceREST.class.getName(),
                optsSupplier.get(),
                supplyDeployHandle.apply(closeVertx, ServiceREST.class.getSimpleName())
        );
    }
}
