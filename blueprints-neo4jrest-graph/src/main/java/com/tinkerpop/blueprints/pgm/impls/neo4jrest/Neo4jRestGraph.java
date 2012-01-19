package com.tinkerpop.blueprints.pgm.impls.neo4jrest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.IndexableGraph;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import com.tinkerpop.blueprints.pgm.impls.neo4jrest.util.Neo4jRestEdgeSequence;
import com.tinkerpop.blueprints.pgm.impls.neo4jrest.util.Neo4jRestVertexSequence;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.rest.graphdb.entity.RestEntity;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.entity.RestRelationship;
import org.neo4j.rest.graphdb.index.RestIndexManager;
import org.neo4j.rest.graphdb.index.RestNodeIndex;
import org.neo4j.rest.graphdb.index.RestRelationshipIndex;
import org.neo4j.rest.graphdb.query.RestGremlinQueryEngine;
import org.neo4j.rest.graphdb.util.QueryResult;

import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;

/**
 * A Blueprints implementation of the graph database Neo4j REST API (http://neo4j.org)
 *
 * @author Marcin Cieslik (marcin.cieslik@gmail.com)
 */
public class Neo4jRestGraph implements IndexableGraph {
	
	private RestGraphDatabase rawGraph;
	private RestGremlinQueryEngine gremlinQueryEngine;
	
    protected Map<String, Neo4jRestAbstractIndex<?,?>> indices = new HashMap<String, Neo4jRestAbstractIndex<?,?>>();
    protected Map<String, Neo4jRestAutomaticIndex<Neo4jRestVertex, RestNode>> automaticVertexIndices = new HashMap<String, Neo4jRestAutomaticIndex<Neo4jRestVertex, RestNode>>();
    protected Map<String, Neo4jRestAutomaticIndex<Neo4jRestEdge, RestRelationship>> automaticEdgeIndices = new HashMap<String, Neo4jRestAutomaticIndex<Neo4jRestEdge, RestRelationship>>();

	public Neo4jRestGraph(final RestGraphDatabase rawGraph, boolean fresh) {
		this.rawGraph = rawGraph;
		this.gremlinQueryEngine = new RestGremlinQueryEngine(rawGraph.getRestAPI());
		this.loadIndices(fresh);
	}
	
	public Neo4jRestGraph(final RestGraphDatabase rawGraph) {
		this.rawGraph = rawGraph;
		this.gremlinQueryEngine = new RestGremlinQueryEngine(rawGraph.getRestAPI());
		RestIndexManager index = rawGraph.index();
		// load fresh indices only if no other indices are present
		if (index.nodeIndexNames().length + index.relationshipIndexNames().length == 0) {
			this.loadIndices(true);	
		} else {
			this.loadIndices(false);
		}
	}	

	public Neo4jRestGraph(final String uri, String user, String password, boolean fresh) {
		this(new RestGraphDatabase(uri, user, password), fresh);	
	}	

	public Neo4jRestGraph(final String uri, String user, String password) {
		this(new RestGraphDatabase(uri, user, password));		
	}	

	public Neo4jRestGraph(final String uri, boolean fresh) {
		this(uri, null, null, fresh);
	}
	
	public Neo4jRestGraph(final String uri) {
		this(uri, null, null);
	}
	
	public Vertex addVertex(Object id) {
		final Vertex vertex = new Neo4jRestVertex((RestNode) this.rawGraph.createNode(), this);
		return vertex;
	}

	public Vertex getVertex(Object id) {
        if (null == id)
            throw new IllegalArgumentException("Element identifier cannot be null");
        try {
            final Long longId;
            if (id instanceof Long)
                longId = (Long) id;
            else
                longId = Double.valueOf(id.toString()).longValue();
            return new Neo4jRestVertex((RestNode) this.rawGraph.getNodeById(longId), this);        
        } catch (NotFoundException e) {
            return null;
        } catch (NumberFormatException e) {
            return null;
        }
	}

	public void removeVertex(Vertex vertex) {
        final Long id = (Long) vertex.getId();
        final RestNode node = (RestNode) this.rawGraph.getNodeById(id);
        if (null != node) {
            try {
                //AutomaticIndexHelper.removeElement(this, vertex);
                for (final Edge edge : vertex.getInEdges()) {
                    ((RestRelationship) ((Neo4jRestEdge) edge).getRawElement()).delete();
                }
                for (final Edge edge : vertex.getOutEdges()) {
                    ((RestRelationship) ((Neo4jRestEdge) edge).getRawElement()).delete();
                }
                node.delete();
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }		
	}

	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        try {
            final RestNode outNode = ((Neo4jRestVertex) outVertex).getRawVertex();
            final RestNode inNode = ((Neo4jRestVertex) inVertex).getRawVertex();
            final RestRelationship relationship = (RestRelationship) outNode.createRelationshipTo(inNode, DynamicRelationshipType.withName(label));
            final Edge edge = new Neo4jRestEdge(relationship, this, true);
            return edge;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
	}

	public Edge getEdge(Object id) {
        if (null == id)
            throw new IllegalArgumentException("Element identifier cannot be null");
        try {
            final Long longId;
            if (id instanceof Long)
                longId = (Long) id;
            else
                longId = Double.valueOf(id.toString()).longValue();
            return new Neo4jRestEdge((RestRelationship) this.rawGraph.getRelationshipById(longId), this);
        } catch (NotFoundException e) {
            return null;
        } catch (NumberFormatException e) {
            return null;
        }
	}

	public void removeEdge(Edge edge) {
        try {
            // AutomaticIndexHelper.removeElement(this, edge);
            ((RestRelationship) ((Neo4jRestEdge) edge).getRawElement()).delete();
        } catch (Exception e) {
        	throw new RuntimeException(e.getMessage(), e);
        }
	}

	public void shutdown() {
        this.rawGraph.shutdown();
	}

    public RestGraphDatabase getRawGraph() {
        return this.rawGraph;
    }

    public String toString() {
        return StringFactory.graphString(this, this.rawGraph.toString());
    }

    // depend on server-side Gremlin
    
	public Iterable<Vertex> getVertices() {
		QueryResult<Object> query = this.gremlinQueryEngine.query("g.V", null);
		return new Neo4jRestVertexSequence(query, this);
	}
	
    public Iterable<Edge> getEdges() {
    	QueryResult<Object> query = this.gremlinQueryEngine.query("g.E", null);
    	return new Neo4jRestEdgeSequence(query, this);
    }
    
	public void clear() {
		this.gremlinQueryEngine.query("g.clear()", null);
	}

	
	// index
	
	public <T extends Element> Index<T> createManualIndex(String indexName,
			Class<T> indexClass) {
        if (this.indices.containsKey(indexName))
            throw new RuntimeException("Index already exists: " + indexName);

        Neo4jRestManualIndex index = new Neo4jRestManualIndex(indexName, indexClass, this);
        this.indices.put(index.getIndexName(), index);
        return index;
	}

	public <T extends Element> AutomaticIndex<T> createAutomaticIndex(
			String indexName, Class<T> indexClass, Set<String> indexKeys) {
        if (this.indices.containsKey(indexName))
            throw new RuntimeException("Index already exists: " + indexName);

        final Neo4jRestAutomaticIndex index = new Neo4jRestAutomaticIndex(indexName, indexClass, this, indexKeys);
        if (Vertex.class.isAssignableFrom(indexClass))
            this.automaticVertexIndices.put(index.getIndexName(), index);
        else
            this.automaticEdgeIndices.put(index.getIndexName(), index);
        this.indices.put(index.getIndexName(), index);
        return index;
	}

	public <T extends Element> Index<T> getIndex(String indexName,
			Class<T> indexClass) {
        final Index index = this.indices.get(indexName);
        // todo: be sure to do code for multiple connections interacting with graph
        if (null == index)
            return null;
        else if (indexClass.isAssignableFrom(index.getIndexClass()))
            return (Index<T>) index;
        else
            throw new RuntimeException("Can not convert " + index.getIndexClass() + " to " + indexClass);
	}

	public Iterable<Index<? extends Element>> getIndices() {
        List<Index<? extends Element>> list = new ArrayList<Index<? extends Element>>();
        for (final Index index : this.indices.values()) {
            list.add(index);
        }
        return list;
    }
	
    protected <T extends Neo4jRestElement> Iterable<Neo4jRestAutomaticIndex<T, RestEntity>> getAutoIndices(final Class<T> indexClass) {
        if (Vertex.class.isAssignableFrom(indexClass))
            return (Iterable) automaticVertexIndices.values();
        else
            return (Iterable) automaticEdgeIndices.values();
    }
	
    public synchronized void dropIndex(final String indexName) {
        try {
            this.rawGraph.index().forNodes(indexName).delete();
            this.rawGraph.index().forRelationships(indexName).delete();
            this.indices.remove(indexName);
            this.automaticEdgeIndices.remove(indexName);
            this.automaticVertexIndices.remove(indexName);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    private void loadIndices(boolean fresh) {
        if (fresh) {
            // remove reference node
            try {
                this.removeVertex(this.getVertex(0));
            } catch (Exception e) {
            }
            this.createAutomaticIndex(Index.VERTICES, Neo4jRestVertex.class, null);
            this.createAutomaticIndex(Index.EDGES, Neo4jRestEdge.class, null);
            return;
        }
        final RestIndexManager manager = this.rawGraph.index();
        for (final String indexName : manager.nodeIndexNames()) {
            final RestNodeIndex neo4jIndex = (RestNodeIndex) manager.forNodes(indexName);
            final String type = manager.getConfiguration(neo4jIndex).get(Neo4jRestTokens.BLUEPRINTS_TYPE);
            if (null != type && type.equals(Index.Type.AUTOMATIC.toString()))
                this.createAutomaticIndex(indexName, Neo4jRestVertex.class, null);
            else
                this.createManualIndex(indexName, Neo4jRestVertex.class);
        }
        
        for (final String indexName : manager.relationshipIndexNames()) {
            final RestRelationshipIndex neo4jIndex = (RestRelationshipIndex) manager.forRelationships(indexName);
            final String type = manager.getConfiguration(neo4jIndex).get(Neo4jRestTokens.BLUEPRINTS_TYPE);
            if (null != type && type.equals(Index.Type.AUTOMATIC.toString()))
                this.createAutomaticIndex(indexName, Neo4jRestEdge.class, null);
            else
                this.createManualIndex(indexName, Neo4jRestEdge.class);
        }
    }
}

