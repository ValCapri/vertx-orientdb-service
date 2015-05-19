package eu.tradegrid.tinkerpop.persistor;

import eu.tradegrid.tinkerpop.persistor.impl.TinkerpopServiceImpl;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.ProxyIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ProxyHelper;

/**
 * Created by lionelschinckus on 7/05/15.
 */
@VertxGen
@ProxyGen
public interface TinkerpopService {
    static TinkerpopService create(Vertx vertx, JsonObject config) {
        JsonObject tinkerpopConfig = config.getJsonObject("tinkerpopConfig");

        if (tinkerpopConfig == null) {
            throw new IllegalStateException("tinkerpopConfig is not present in config");
        }

        return new TinkerpopServiceImpl(vertx, tinkerpopConfig);
    }

    static TinkerpopService createEventBusProxy(Vertx vertx, String address) {
        return ProxyHelper.createProxy(TinkerpopService.class,vertx, address);
    }

    @ProxyIgnore
    void start();

    @ProxyIgnore
    void stop();

    void addGraph(JsonObject graphJson, Handler<AsyncResult<JsonObject>> resultHandler);

    void addVertex(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler);

    @SuppressWarnings("unchecked")
    void queryWithId(String id, String starts, String query, boolean cache, Handler<AsyncResult<JsonObject>> resultHandler);

    void queryGremlin(String query, Handler<AsyncResult<JsonObject>> resultHandler);

    void getVertices(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler);

    void getVerticesOfClass(String aClass, Handler<AsyncResult<JsonObject>> resultHandler);

    void getVertex(String id, Handler<AsyncResult<JsonObject>> resultHandler);

    void removeVertex(String id, Handler<AsyncResult<JsonObject>> resultHandler);

    void addEdge(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler);

    void getEdge(String id, Handler<AsyncResult<JsonObject>> resultHandler);

    void getEdges(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler);

    void removeEdge(String id, Handler<AsyncResult<JsonObject>> resultHandler);

    void createKeyIndex(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler);

    void dropKeyIndex(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler);

    void getIndexedKeys(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler);

    void flushQueryCache(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler);
}
