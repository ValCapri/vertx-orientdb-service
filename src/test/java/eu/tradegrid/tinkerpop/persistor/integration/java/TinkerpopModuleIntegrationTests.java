/*
 * Copyright (c) 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.tradegrid.tinkerpop.persistor.integration.java;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import eu.tradegrid.tinkerpop.persistor.TinkerpopServiceVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.test.core.TestVerticle;
import io.vertx.test.core.VertxTestBase;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.io.IOException;
import java.io.InputStream;


/**
 * Tinkerpop Persistor Bus Module integration tests for 
 * <a href="http://www.neo4j.org/">Neo4J</a> and
 * <a href="https://github.com/orientechnologies/orientdb">OrientDB</a>.
 * <p/>
 * @author <a href="https://github.com/aschrijver">Arnold Schrijver</a>
 */
public class TinkerpopModuleIntegrationTests extends VertxTestBase {
    
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private JsonObject config;
    private EventBus rxEventBus;
    
    Object vertexId;
    Object outVertex;
    private Vertx rxVertx;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        rxVertx = new Vertx(super.vertx);
        rxEventBus = rxVertx.eventBus();
        
        try {
            tempFolder.create();
        } catch (IOException e) {
            fail("Cannot open temporary folder for graph database.");
        }
        
        //config = getNeo4jConfig();
        config = getOrientDbConfig();

        super.vertx.deployVerticle(TinkerpopServiceVerticle.class.getName(), new DeploymentOptions().setConfig(config), res -> {
            assertTrue(res.succeeded());
            assertNotNull("deploymentID should not be null", res.result());

            //startTests();
        });

    }

    //@Test
    public void testServer() {


        TransactionalGraph graph = new OrientGraph("remote:localhost/test$customers", "admin", "admin");
        Vertex vPerson = graph.addVertex("class:Person");
        vPerson.setProperty("firstName", "John");
        vPerson.setProperty("lastName", "Smith");

        Vertex vAddress = graph.addVertex("class:Address");
        vAddress.setProperty("street", "Van Ness Ave.");
        vAddress.setProperty("city", "San Francisco");
        vAddress.setProperty("state", "California");

    }
    
    /*@Test
    public void testAddGetAndRemoveVertex() {

        // Load sample GraphSON message.
        JsonObject vertexToAdd = getResourceAsJson("addVertex.json");
        
        final Action1<Exception> failure = e -> fail(e.getMessage());
        
        final Action1<Message<JsonObject>> testComplete = message -> {
            assertEquals("ok", message.body().getString("status"));
            assertEquals(vertexId, message.body().getValue("_id"));
            testComplete();
        };
        
        final Func1<Message<JsonObject>, Observable<Message<JsonObject>>> removeVertex =
                message -> {
                    assertEquals("ok", message.body().getString("status"));
                    assertNotNull("GraphSON: 'graph' object missing", message.body().getJsonObject("graph"));
                    assertNotNull("GraphSON: 'vertices' array missing", message.body().getJsonObject("graph").getJsonArray("vertices"));
                    assertEquals(1, message.body().getJsonObject("graph").getJsonArray("vertices").size());
                    assertTrue(message.body().getJsonObject("graph").getJsonArray("vertices").getJsonObject(0) instanceof JsonObject);

                    JsonObject vertex = message.body().getJsonObject("graph").getJsonArray("vertices").getJsonObject(0);
                    assertEquals("vert.x", vertex.getString("project"));
                    assertNotNull(vertex.getValue("_id"));
                    vertexId = vertex.getValue("_id");

                    JsonObject vertexToRemove = new JsonObject();
                    vertexToRemove.put("action", "removeVertex");
                    vertexToRemove.put("_id", vertexId);

                    rxEventBus.send("test.persistor", vertexToRemove);
                };
        
        final Func1<Message<JsonObject>, Observable<Message<JsonObject>>> getVertex =
                message -> {
                    assertEquals("ok", message.body().getString("status"));
                    assertNotNull(message.body().getValue("_id"));

                    Object addedVertexId = message.body().getValue("_id");

                    // NOTE: The id returned depends on the graphdb implementation used and is not
                    //       guaranteed to be equal to the one provided in the GraphSON message.
                    assertNotNull("AddVertex must return a Vertex Id", addedVertexId);

                    JsonObject vertexToGet = new JsonObject();
                    vertexToGet.put("action", "getVertex");
                    vertexToGet.put("_id", addedVertexId);

                    rxEventBus.send("test.persistor", vertexToGet);
                };
        
        rxEventBus.send("test.persistor", vertexToAdd)
                .(getVertex)
                .mapMany(removeVertex)
                .subscribe(testComplete, failure);   
    }
    
    @Test
    public void testAddVertexAndEdge() {
        
        // Load sample GraphSON message derived from Neo4J documentation.
        JsonObject graphToAdd = getResourceAsJson("neo4jAclGraphExample.json");
        JsonObject message = new JsonObject().putString("action", "addGraph")
                .putObject("graph", graphToAdd);
        
        vertx.eventBus().send("test.persistor", message, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(Message<JsonObject> message) {
                JsonObject reply = message.body();
                assertEquals("ok", reply.getString("status"));
                
                final JsonObject getVertex = new JsonObject()
                        .putString("action", "getVertices")
                        .putString("key", "name")
                        .putString("value", "User1 Home");
        
                vertx.eventBus().send("test.persistor", getVertex, new Handler<Message<JsonObject>>() {
        
                    @Override
                    public void handle(Message<JsonObject> message) {
                        JsonObject reply = message.body();
                        assertEquals("ok", reply.getString("status"));
                        assertNotNull(((JsonObject) message.body().getJsonObject("graph")
                                .getJsonArray("vertices").get(0)).getValue("_id"));

                        outVertex = ((JsonObject) message.body().getJsonObject("graph")
                                .getJsonArray("vertices").get(0)).getValue("_id");
                        
                        final JsonObject addVertex = new JsonObject()
                                .putString("action", "addVertex")
                                .putArray("vertices", new JsonArray()
                                        .addObject(new JsonObject()
                                                .putString("name", "My File2.pdf")));
                        
                        vertx.eventBus().send("test.persistor", addVertex, new Handler<Message<JsonObject>>() {
                            
                            @Override
                            public void handle(Message<JsonObject> message) {
                                JsonObject reply = message.body();
                                assertEquals("ok", reply.getString("status"));
                                
                                Object inVertex = reply.getValue("_id");
                                assertNotNull(inVertex);
                                
                                final JsonObject addEdge = new JsonObject()
                                        .putString("action", "addEdge")
                                        .putArray("edges", new JsonArray()
                                                .addObject(new JsonObject()
                                                        .putValue("_inV", inVertex)
                                                        .putValue("_outV", outVertex)
                                                        .putString("_label", "HAS_CHILD_CONTENT")));
                                
                                vertx.eventBus().send("test.persistor", addEdge, new Handler<Message<JsonObject>>() {
                                    
                                    @Override
                                    public void handle(Message<JsonObject> message) {
                                        JsonObject reply = message.body();
                                        assertEquals("ok", reply.getString("status"));
                                        assertNotNull(reply.getValue("_id"));
                                        
                                        testComplete();
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });
    }
    
    @Test
    public void testAddGraph() {
        
        // Load sample GraphSON message derived from Neo4J documentation.
        JsonObject graphToAdd = getResourceAsJson("neo4jAclGraphExample.json");
        JsonObject message = new JsonObject().putString("action", "addGraph")
                .putObject("graph", graphToAdd);
        
        vertx.eventBus().send("test.persistor", message, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(Message<JsonObject> message) {
                JsonObject reply = message.body();
                
                assertEquals("ok", reply.getString("status"));
                
                // Graph is only returned if ID's were generated by the db
                if (reply.getJsonObject("graph") != null) {
                    assertNotNull("GraphSON: 'vertices' array missing", message.body().getJsonObject("graph").getJsonArray("vertices"));
                    assertNotNull("GraphSON: 'edges' array missing", message.body().getJsonObject("graph").getJsonArray("edges"));
//                    assertEquals(5, message.body().getJsonObject("graph").getJsonArray("edges").size());
                    
                    JsonArray vertices = message.body().getJsonObject("graph").getJsonArray("vertices");
                    if (config.getJsonObject("tinkerpopConfig").getString("blueprints.neo4j.directory") != null) {

                        // In Neo4J we have to account for auto-generated root vertex.
                        assertEquals(13, vertices.size());
                    } else {
                       // assertEquals(147, vertices.size());
                    }
                    
                    assertTrue(message.body().getJsonObject("graph").getJsonArray("vertices").get(0) instanceof JsonObject);
                    assertTrue(message.body().getJsonObject("graph").getJsonArray("edges").get(0) instanceof JsonObject);
                }

                testComplete();
            }            
        });
    }
    
    @Test
    public void testQueryGraph() {
        final String query = "_().in('HAS_CHILD_CONTENT').loop(1){it.loops < 3}{it.object.name == 'Root folder'}.path";
        
        // Load sample GraphSON message derived from Neo4J documentation.
        JsonObject graphToAdd = getResourceAsJson("neo4jAclGraphExample.json");
        JsonObject message = new JsonObject().putString("action", "addGraph")
                .putObject("graph", graphToAdd);
        
        vertx.eventBus().send("test.persistor", message, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(Message<JsonObject> message) {
                JsonObject reply = message.body();
                assertEquals("ok", reply.getString("status"));
                
                final JsonObject getStartVertex = new JsonObject()
                        .putString("action", "getVertices")
                        .putString("key", "name")
                        .putString("value", "User1 Home");
                
                vertx.eventBus().send("test.persistor", getStartVertex, new Handler<Message<JsonObject>>() {

                    @Override
                    public void handle(Message<JsonObject> message) {
                        JsonObject reply = message.body();
                        assertEquals("ok", reply.getString("status"));
                        assertNotNull("GraphSON: 'graph' object missing", message.body().getJsonObject("graph"));
                        assertNotNull("GraphSON: 'vertices' array missing", message.body().getJsonObject("graph").getJsonArray("vertices"));
                        assertEquals(1, message.body().getJsonObject("graph").getJsonArray("vertices").size());
                        
                        final Object id = ((JsonObject) reply.getJsonObject("graph").getJsonArray("vertices").get(0)).getValue("_id");
                        final JsonObject queryRootFolder = new JsonObject()
                                .putString("action", "query")
                                .putString("query", query)
                                .putValue("_id", id);
                        
                        vertx.eventBus().send("test.persistor", queryRootFolder, new Handler<Message<JsonObject>>() {

                            @Override
                            public void handle(Message<JsonObject> message) {
                                JsonObject reply = message.body();
                                assertEquals("ok", reply.getString("status"));
                                
                                assertNotNull(reply.getJsonArray("results"));
                                assertEquals(3, reply.getJsonArray("results").size());
                                
                                testComplete();
                            }
                        });
                    }
                });
            }
        });
    }

    @Test
    public void testAddGetAndDropKeyIndex() {

        // Load sample GraphSON message derived from Neo4J documentation.
        JsonObject graphToAdd = getResourceAsJson("neo4jAclGraphExample.json");
        JsonObject message = new JsonObject().putString("action", "addGraph")
                .putObject("graph", graphToAdd);
        
        vertx.eventBus().send("test.persistor", message, new Handler<Message<JsonObject>>() {

            @Override
            public void handle(Message<JsonObject> message) {
                JsonObject reply = message.body();
                assertEquals("ok", reply.getString("status"));

                JsonObject createKeyIndex = new JsonObject()
                        .putString("action", "createKeyIndex")
                        .putString("key", "name")
                        .putString("elementClass", "Vertex");
                
                vertx.eventBus().send("test.persistor", createKeyIndex, new Handler<Message<JsonObject>>() {

                    @Override
                    public void handle(Message<JsonObject> message) {
                        JsonObject reply = message.body();
                        assertEquals("ok", reply.getString("status"));
                        
                        JsonObject getIndexedKeys = new JsonObject()
                                .putString("action", "getIndexedKeys")
                                .putString("elementClass", "Vertex");
                        
                        vertx.eventBus().send("test.persistor", getIndexedKeys, new Handler<Message<JsonObject>>() {

                            @Override
                            public void handle(Message<JsonObject> message) {
                                JsonObject reply = message.body();
                                assertEquals("ok", reply.getString("status"));
                                assertNotNull(reply.getJsonArray("keys"));
                                assertEquals(1, reply.getJsonArray("keys").size());
                                assertEquals("name", reply.getJsonArray("keys").get(0));
                                
                                JsonObject dropKeyIndex = new JsonObject()
                                        .putString("action", "dropKeyIndex")
                                        .putString("key", "name")
                                        .putString("elementClass", "Vertex");

                                vertx.eventBus().send("test.persistor", dropKeyIndex, new Handler<Message<JsonObject>>() {

                                    @Override
                                    public void handle(Message<JsonObject> message) {
                                        JsonObject reply = message.body();
                                        assertEquals("ok", reply.getString("status"));
                                        testComplete();
                                    }
                                });
                            }                            
                        });
                    }
                });
            }
        });
    }
    
    private JsonObject getNeo4jConfig() {
        JsonObject neo4jConfig = new JsonObject();
        neo4jConfig.put(
                "blueprints.graph", "com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph");
        neo4jConfig.put("blueprints.neo4j.directory", tempFolder.getRoot().getPath());

        JsonObject config = new JsonObject();
        config.put("address", "test.persistor");
        config.put("tinkerpopConfig", neo4jConfig);
        
        return config;
    }*/
    
    private JsonObject getOrientDbConfig() {
        JsonObject orientDbConfig = new JsonObject();
        orientDbConfig.put(
                "blueprints.graph", "com.tinkerpop.blueprints.impls.orient.OrientGraph");
        //orientDbConfig.putString("blueprints.orientdb.url", "remote:" + "localhost/rds");
        orientDbConfig.put("blueprints.orientdb.url", "plocal:" + "D://database/rds");

        // TODO: Add your own user here in <orientdb-location>/config/orientdb-server-config.xml
        orientDbConfig.put("blueprints.orientdb.username", "root");
        orientDbConfig.put("blueprints.orientdb.password", "root");

        // New configuration options (available in v1.6.0-SNAPSHOT).
        orientDbConfig.put("blueprints.orientdb.saveOriginalIds", true);
        orientDbConfig.put("blueprints.orientdb.lightweightEdges", true);
        
        JsonObject config = new JsonObject();
        config.put("address", "test.persistor");
        config.put("tinkerpopConfig", orientDbConfig);
        
        return config;        
    }
    
    private JsonObject getResourceAsJson(String filename) {
        InputStream is = ClassLoader.getSystemResourceAsStream(filename);
        String jsonData = null;
        try {
            jsonData = IOUtils.toString(is, "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new JsonObject(jsonData);
    }

}
