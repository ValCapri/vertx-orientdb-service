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

package eu.tradegrid.tinkerpop.persistor.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.graph.gremlin.OCommandGremlin;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.gremlin.groovy.Gremlin;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.iterators.SingleIterator;
import eu.tradegrid.tinkerpop.persistor.TinkerpopService;
import eu.tradegrid.tinkerpop.persistor.util.JsonUtility;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.EncodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import org.apache.commons.configuration.MapConfiguration;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tinkerpop Persistor Bus Module
 * <p/>
 * This module uses <a href="https://github.com/tinkerpop/blueprints">Tinkerpop Blueprints</a> and
 * <a href="https://github.com/tinkerpop/gremlin">Tinkerpop Gremlin</a> to retrieve and persist 
 * data in a supported graph database.
 * <p/>
 * Please see the README.md for more detailed information.
 * <p/> 
 * @author <a href="https://github.com/aschrijver">Arnold Schrijver</a>
 */
public class TinkerpopServiceImpl implements TinkerpopService {

    private final Vertx vertx;
    private JsonObject tinkerpopConfig;
    private JsonUtility jsonUtility;
    
    private ConcurrentHashMap<String, Pipe<Element, Object>> queryCache;
    private Logger logger = LoggerFactory.getLogger(TinkerpopServiceImpl.class);
    private OrientGraph graph;

    public TinkerpopServiceImpl(Vertx vertx, JsonObject tinkerpopConfig) {
        this.vertx = vertx;
        this.tinkerpopConfig = tinkerpopConfig;
    }

    /**
     * Start the Tinkerpop Persistor module.
     */
    @Override
    public void start() {
        jsonUtility = new JsonUtility(tinkerpopConfig.getString("graphson.mode", "NORMAL"));
        
        queryCache = new ConcurrentHashMap<>();

        logger.info("TinkerpopServiceImpl module started");

        graph = new OrientGraph(new MapConfiguration(tinkerpopConfig.getMap()));
    }
    
    /**
     * Stop the Tinkerpop Persistor module.
     */
    @Override
    public void stop() {
        graph.shutdown();
        logger.info("TinkerpopServiceImpl module stopped");
    }
    
    /**
     * Add a complete {@link Graph} to the db that may consist of multiple vertices and
     * edges. The graph in the message body must follow the GraphSON format.</p>
     * 
     * @param message the message containing information on the full Graph to create
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    @Override
    public void addGraph(JsonObject graphJson, Handler<AsyncResult<JsonObject>> resultHandler) {
        if (graphJson == null) {
            resultHandler.handle(Future.failedFuture("Action 'addGraph': No graphSON data supplied."));
            return;
        }
        
        try {
            jsonUtility.deserializeGraph(graph, graphJson);
        } catch (UnsupportedEncodingException e) {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'addGraph': The Graphson message is not UTF-8 encoded", e)));
            return;
        } catch (IOException e) {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'addGraph': The Graphson message is invalid", e)));
            return;
        }
        
        if (graph instanceof TransactionalGraph) {
            ((TransactionalGraph) graph).commit();
        }

        // Need to return the resulting Graph, if Id's have been generated.
        if (graph.getFeatures().ignoresSuppliedIds) {
            JsonObject reply;
            try {
                reply = jsonUtility.serializeGraph(graph);
            } catch (IOException e) {
                resultHandler.handle(Future.failedFuture(new Exception("Action 'addGraph': Cannot convert Graph to JSON", e)));
                return;
            }
            
            resultHandler.handle(Future.succeededFuture(reply));
        } else {
            resultHandler.handle(Future.succeededFuture());
        }
    }
    
    /**
     * Add a new {@link Vertex} to the db and return a reply  with the Id of the 
     * newly created vertex. The vertex in the message body must follow the GraphSON format.
     * Only the first Vertex in the GraphSON message is processed, other vertices and
     * edges are ignored.<p/>
     * 
     * Note that the id is not guaranteed to be similar to that received in the incoming,
     * {@link Message} since Id generation may be database-vendor-specific.<p/>
     * 
     * If an error occurs and the graph was transactional, then a rollback will occur.<p/>
     * 
     * @param message the message containing information on the new Vertex to create
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    @Override
    public void addVertex(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler) {
        JsonArray verticesJson = message.getJsonArray("vertices");
        if (verticesJson == null || verticesJson.size() == 0) {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'addVertex': No vertex data supplied.")));
            return;
        }
        
        Vertex vertex;
        try {
            vertex = jsonUtility.deserializeVertex(graph, verticesJson.getJsonObject(0));
        } catch (IOException e) {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'addVertex': The Graphson message is invalid", e)));
            return;
        }

        if (graph instanceof TransactionalGraph) {
            ((TransactionalGraph) graph).commit();
        } else if (graph.getFeatures().ignoresSuppliedIds) {
            // Shutting down the graph should force Id generation.
            graph.shutdown();
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug("Added Vertex with Id: " + vertex.getId().toString());
        }

        JsonObject reply = new JsonObject().put("_id", vertex.getId().toString());
        try {
            resultHandler.handle(Future.succeededFuture(reply));
        } catch (EncodeException e) {
            // Id is not a Json support datatype, serialize to string.
            reply.put("_id", vertex.getId().toString());
            resultHandler.handle(Future.succeededFuture(reply));
        }
    }

    /**
     * Execute a Gremlin query starting from the {@link Vertex} or {@link Edge} specified by Id in
     * the message body, and by using the query string specified in the 'query' field.<p/>
     * The query will first be compiled to a Gremlin {@link Pipe} which is then iterated and
     * returned as JSON in the message reply.
     * <p/>
     * Currently there is only support for queries that deal with either {@link Vertex} or {@link Edge}
     * for their starts (and ends) types.
     *
     * @param message the message containing information on the Gremlin query to execute
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    @Override
    @SuppressWarnings("unchecked")
    public void queryWithId(String id, String starts, String query, boolean cache, Handler<AsyncResult<JsonObject>> resultHandler) {
        if (id == null) {
            resultHandler.handle(Future.failedFuture("id connot be null"));
            return;
        }

        if (query == null) {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'query': No query specified.")));
            return;
        }

        Element element = null;
        if ("Vertex".equals(starts) == true) {
            element = graph.getVertex(id);
        } else if ("Edge".equals(starts)) {
            element = graph.getEdge(id);
        } else {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'query': Unsupported starts property: " + starts)));
            return;
        }

        if (element == null) {
            resultHandler.handle(Future.failedFuture(new Exception(String.format("Action 'query': Starting %s %s not found",
                    starts, id.toString()))));
            return;
        }

        Pipe<Element, Object> pipe = null;
        if (queryCache.containsKey(query)) {
            pipe = queryCache.get(query);
        } else {
            try {
                pipe = Gremlin.compile(query);
            } catch (Exception e) {
                resultHandler.handle(Future.failedFuture(new Exception("Action 'query': Cannot compile query.", e)));
                return;
            }

            if (cache) {
                queryCache.put(query, pipe);
            }
        }

        pipe.setStarts(new SingleIterator<Element>(element));

        JsonArray queryResults;
        try {
            queryResults = jsonUtility.serializePipe(pipe);
        } catch (IOException e) {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'query': Error converting Pipe to JSON.", e)));
            return;
        }

        JsonObject reply = new JsonObject();
        reply.put("results", queryResults);

        resultHandler.handle(Future.succeededFuture(reply));
    }

    /**
     *
     * @param query
     * @param resultHandler
     */
    public void queryGremlin(String query, Handler<AsyncResult<JsonObject>> resultHandler) {
        Object results = graph.command(new OCommandGremlin(query)).execute();
        JsonObject reply = null;

        if (results instanceof OrientDynaElementIterable) {
            try {
                reply = jsonUtility.convertOrientDynaElementIterableToJson((OrientDynaElementIterable) results);
            } catch (IOException e) {
                resultHandler.handle(Future.failedFuture(e));
            }
        } else if (results instanceof Element) {
            try {
                JsonObject graphJsonObject = new JsonObject();
                if (results instanceof Vertex) {
                    graphJsonObject.put("vertices", new JsonArray().add(jsonUtility.serializeElement((Vertex)results)));
                } else if (results instanceof Edge) {
                    graphJsonObject.put("vertices", new JsonArray().add(jsonUtility.serializeElement((Edge)results)));
                }

                reply = new JsonObject().put("graph",graphJsonObject);
            } catch (IOException e) {
                resultHandler.handle(Future.failedFuture(e));
            }
        }

        resultHandler.handle(Future.succeededFuture(reply));
    }

    /**
     * Retrieve one or more vertices from the db in a single call. The {@link Message}
     * may contain optional 'key' and a 'value' fields to filter only on those vertices that
     * have the specified key/value pair.<p/>
     *
     * @param message the message containing information on the vertices to retrieve
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    @Override
    public void getVertices(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler) {
        String key = message.getString("key");
        Object value = message.getValue("value");

        JsonArray verticesJson;
        try {
            if (key == null) {
                verticesJson = jsonUtility.serializeElements(graph.getVertices());
            } else if (value != null) {
                verticesJson = jsonUtility.serializeElements(graph.getVertices(key, value));
            } else {
                resultHandler.handle(Future.failedFuture(new Exception("Action 'getVertices': Both a key and a value must be specified")));
                return;
            }
        } catch (IOException e) {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'getVertices': Cannot convert vertices to JSON", e)));
            return;
        }

        JsonObject reply = new JsonObject().put("graph", new JsonObject()
                .put("mode", jsonUtility.getGraphSONMode())
                .put("vertices", verticesJson));

        resultHandler.handle(Future.succeededFuture(reply));
    }

    /**
     * Retrieve one or more vertices from the db in a single call. The {@link Message}
     * may contain optional 'key' and a 'value' fields to filter only on those vertices that
     * have the specified key/value pair.<p/>
     *
     * @param aClass a string of the custom type
     * @param resultHandler the result handler
     */
    @Override
    public void getVerticesOfClass(String aClass, Handler<AsyncResult<JsonObject>> resultHandler) {
        JsonArray verticesJson;
        try {
            if (aClass == null) {
                verticesJson = jsonUtility.serializeElements(graph.getVertices());
            } else {
                verticesJson = jsonUtility.serializeElements(graph.getVerticesOfClass(aClass));
            }
        } catch (IOException e) {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'getVertices': Cannot convert vertices to JSON", e)));
            return;
        }

        JsonObject reply = new JsonObject().put("graph", new JsonObject()
                .put("mode", jsonUtility.getGraphSONMode())
                .put("vertices", verticesJson));

        resultHandler.handle(Future.succeededFuture(reply));
    }

    /**
     * Retrieve the {@link Vertex} with the id specified in the {@link Message}.<p/>
     * 
     * @param message the message containing information on the Vertex to retrieve
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    @Override
    public void getVertex(String id, Handler<AsyncResult<JsonObject>> resultHandler) {
        getElement(id, resultHandler, "Vertex");
    }
    
    /**
     * Remove the {@link Vertex} with the id specified in the {@link Message} from the database.
     * <p/>
     * 
     * @param message the message containing information on the Vertex to remove
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    @Override
    public void removeVertex(String id, Handler<AsyncResult<JsonObject>> resultHandler) {
        removeElement(id, resultHandler, "Vertex");
    }
    
    /**
     * Add a new {@link Edge} to the db and return a reply  with the Id of the 
     * newly created edge. The edge in the message body must follow the GraphSON format.
     * Only the first Edge in the GraphSON message is processed, other edges and
     * vertices are ignored.<p/>
     * 
     * The vertices for both the _inV and _outV vertex id's specified in the GraphSON message
     * must both exist in the db.
     * 
     * Note that the id is not guaranteed to be similar to that received in the incoming,
     * {@link Message} since Id generation may be database-vendor-specific.<p/>
     * 
     * If an error occurs and the graph was transactional, then a rollback will occur.<p/>
     * 
     * @param message the message containing information on the new Edge to create
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    @Override
    public void addEdge(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler) {

        // Formatted according to GraphSON format.
        JsonArray edgesJson = message.getJsonArray("edges");
        JsonObject edgeJson = edgesJson.getJsonObject(0);
        Object inId = edgeJson.getValue("_inV");
        Object outId = edgeJson.getValue("_outV");
        
        // The label is a required field in some database products.
        if (edgeJson.getString("_label") == null) {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'addEdge': Key _label is a required field")));
            return;
        }
        
        Vertex inVertex = graph.getVertex(inId);
        Vertex outVertex = graph.getVertex(outId);
        Edge edge;
        
        try {
            edge = jsonUtility.deserializeEdge(graph, inVertex, outVertex, edgeJson);
        } catch (IOException e) {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'addEdge': The Graphson message is invalid", e)));
            return;
        }
        
        if (graph instanceof TransactionalGraph) {
            ((TransactionalGraph) graph).commit();
        } else if (graph.getFeatures().ignoresSuppliedIds) {
            // Shutting down the graph should force Id generation.
            graph.shutdown();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Added Edge with Id: " + edge.getId().toString());
        }

        JsonObject reply = new JsonObject().put("_id", edge.getId().toString());

        resultHandler.handle(Future.succeededFuture(reply));
    }
    
    /**
     * Retrieve the {@link Edge} with the id specified in the {@link Message}.<p/>
     * 
     * @param message the message containing information on the Edge to retrieve
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    @Override
    public void getEdge(String id, Handler<AsyncResult<JsonObject>> resultHandler) {
        getElement(id, resultHandler, "Edge");
    }
    
    /**
     * Retrieve one or more edges from the db in a single call. The {@link Message} 
     * may contain optional 'key' and a 'value' fields to filter only on those edges that
     * have the specified key/value pair.<p/>
     * 
     * @param message the message containing information on the edges to retrieve
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    @Override
    public void getEdges(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler) {
        String key = message.getString("key");
        Object value = message.getValue("value");
        
        JsonArray edges;
        try {
            if (key == null) {
                edges = jsonUtility.serializeElements(graph.getEdges());    
            } else if (value != null) {
                edges = jsonUtility.serializeElements(graph.getEdges(key, value));
            } else {
                resultHandler.handle(Future.failedFuture(new Exception("Action 'getEdges': Both a key and a value must be specified")));
                return;
            }
        }
        catch (IOException e) {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'getEdges': Cannot convert Edges to JSON", e)));
            return;
        }
        
        JsonObject reply = new JsonObject().put("graph", new JsonObject()
                .put("mode", jsonUtility.getGraphSONMode())
                .put("edges", edges));
        
         resultHandler.handle(Future.succeededFuture(reply)); 
    }
    
    /**
     * Remove the {@link Edge} with the id specified in the {@link Message} from the database.
     * <p/>
     * 
     * @param message the message containing information on the Edge to remove
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    @Override
    public void removeEdge(String id, Handler<AsyncResult<JsonObject>> resultHandler) {
        removeElement(id, resultHandler, "Edge");
    }
        
    /**
     * Create an index in the underlying graph database based on the provided key and optional
     * parameters.
     * 
     * @param message the message containing information on the Key Index to create
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    @Override
    public void createKeyIndex(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler) {
        
        if (graph.getFeatures().supportsKeyIndices && graph instanceof KeyIndexableGraph) {
            String key = message.getString("key");
            
            Class<? extends Element> elementClass = getIndexElementClass(message);
            if (elementClass == null) {
                resultHandler.handle(Future.failedFuture(new Exception("Action 'createKeyIndex': Unsupported elementClass " + elementClass)));
                return;
            }
            
            Parameter<String, Object>[] parameters = getIndexParameters(message);

            try {
                if (parameters == null) {
                    ((KeyIndexableGraph) graph).createKeyIndex(key, elementClass);
                } else {
                    ((KeyIndexableGraph) graph).createKeyIndex(key, elementClass, parameters);
                }
            } catch (RuntimeException e) {
                resultHandler.handle(Future.failedFuture(new Exception("Action 'createKeyIndex': Cannot create index with key " + key, e)));
            }
            
            resultHandler.handle(Future.succeededFuture());
        } else {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'createKeyIndex': Graph does not support key indices")));
        }
    }

    /**
     * Drop an index in the underlying graph database based on the provided key. The available
     * index keys can be found by first sending a 'getIndexedKeys' action to the persistor.
     * 
     * @param message the message containing information on the Key Index to create
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    @Override
    public void dropKeyIndex(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler) {
        
        if (graph.getFeatures().supportsKeyIndices && graph instanceof KeyIndexableGraph) {
            String key = message.getString("key");

            Class<? extends Element> elementClass = getIndexElementClass(message);
            if (elementClass == null) {
                resultHandler.handle(Future.failedFuture(new Exception("Action 'dropKeyIndex': Unsupported elementClass " + elementClass)));
                return;
            }
            
            try {
                ((KeyIndexableGraph) graph).dropKeyIndex(key, elementClass);
            } catch (RuntimeException e) {
                resultHandler.handle(Future.failedFuture(new Exception("Action 'dropKeyIndex': Cannot drop index with key " + key, e)));
            }
            
            resultHandler.handle(Future.succeededFuture());
        } else {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'dropKeyIndex': Graph does not support key indices")));
        }
    }
    
    /**
     * Get the list of keys for all of the indices that exist in the underlying graph database.
     * 
     * @param message the message that contains the element class for which to retrieve the indices
     * @param graph the Tinkerpop graph that is used to communicate with the underlying graphdb
     */
    @Override
    public void getIndexedKeys(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler) {
        
        if (graph.getFeatures().supportsKeyIndices && graph instanceof KeyIndexableGraph) {
            try {
                Class<? extends Element> elementClass = getIndexElementClass(message);
                if (elementClass == null) {
                    resultHandler.handle(Future.failedFuture(new Exception("Action 'dropKeyIndex': Unsupported elementClass " + elementClass)));
                    return;
                }
                
                Set<String> indexedKeys = ((KeyIndexableGraph) graph).getIndexedKeys(elementClass);
                JsonObject reply = new JsonObject()
                        .put("keys", new JsonArray(new ArrayList(indexedKeys)));

                resultHandler.handle(Future.succeededFuture(reply));
            } catch (RuntimeException e) {
                resultHandler.handle(Future.failedFuture(new Exception("Action 'getIndexedKeys': Cannot retrieve indexed keys", e)));
            }
        } else {
            resultHandler.handle(Future.failedFuture(new Exception("Action 'getIndexedKeys': Graph does not support key indices")));
        }
    }

    @Override
    public void flushQueryCache(JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler) {
        String query = message.getString("query");
        if (query == null) {
            queryCache.clear();
        } else {
            queryCache.remove(query);
        }

        resultHandler.handle(Future.succeededFuture());
    }

    private Class<? extends Element> getIndexElementClass(JsonObject message) {
        String elementClass = message.getString("elementClass");

        switch (elementClass) {
            case "Vertex":
                return Vertex.class;
            case "Edge":
                return Edge.class;
            default:
                return null;
        }        
    }
    
    @SuppressWarnings("unchecked")
    private Parameter<String, Object>[] getIndexParameters(JsonObject message) {
        JsonObject indexParameters = message.getJsonObject("parameters");
        
        Parameter<String, Object>[] parameters = null;
        if (indexParameters != null && indexParameters.size() > 0) {
            parameters = (Parameter<String, Object>[]) indexParameters.getMap().entrySet().toArray();
        }
        
        return parameters;
    }

    private void getElement(String id, Handler<AsyncResult<JsonObject>> resultHandler, String elementType) {
        if (id == null) {
            resultHandler.handle(Future.failedFuture("no id specified for getElement"));
            return;
        }
        
        Element element = elementType.equals("Vertex") ? graph.getVertex(id) : graph.getEdge(id);
        if (element ==  null) {
            resultHandler.handle(Future.failedFuture(new Exception(
                    String.format("Action 'get%s': %s %s not found",
                            elementType, elementType, id.toString()))));
            return;
        }
        
        JsonObject elementJson;
        try {
            elementJson = jsonUtility.serializeElement(element);
        } catch (IOException e) {
            resultHandler.handle(Future.failedFuture(new Exception(String.format(
                    "Action 'get%s': Cannot convert %s %s to JSON",
                    elementType, elementType, element.toString()), e)));
            return;
        }
        
        String arrayElement = Objects.equals(elementType, "Vertex") ? "vertices" : "edges";
        JsonObject reply = new JsonObject().put("graph", new JsonObject()
                .put("mode", jsonUtility.getGraphSONMode())
                .put(arrayElement, new JsonArray()
                        .add(elementJson)));
        
         resultHandler.handle(Future.succeededFuture(reply)); 
    }
    
    private void removeElement(String id, Handler<AsyncResult<JsonObject>> resultHandler, String elementType) {
        if (id == null) {
            resultHandler.handle(Future.failedFuture("no id specified for getElement"));
            return;
        }
        
        Element element = elementType.equals("Vertex") ? graph.getVertex(id) : graph.getEdge(id);
        if (element ==  null) {
            resultHandler.handle(Future.failedFuture(new Exception(
                    String.format("Action 'remove%s': Cannot remove. %s %s not found",
                            elementType, elementType, id.toString()))));
            return;
        }
        
        try {
            if (elementType.equals("Vertex")) {
                graph.removeVertex((Vertex) element);
            } else {
                graph.removeEdge((Edge) element);
            }
        } catch (Exception e) {
            resultHandler.handle(Future.failedFuture(new Exception(
                    String.format("Action 'remove%s': Error removing %s with Id %s",
                            elementType, elementType, id.toString()))));
            return;
        }

        if (graph instanceof TransactionalGraph) {
            ((TransactionalGraph) graph).commit();
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Removed %s with Id %s", elementType, id.toString()));
        }
        
        JsonObject reply = new JsonObject().put("_id", id);
         resultHandler.handle(Future.succeededFuture(reply)); 
    }
    
    private Object getMandatoryValue(JsonObject message, String fieldName) throws Exception {
        Object value = message.getValue(fieldName);
        if (value == null) {
            String action = message.getString("action");
            throw new Exception(String.format("Action '%s': %s must be specified", action, fieldName));
        }
        return value;
    }
}
