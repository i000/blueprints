package com.tinkerpop.blueprints.pgm.impls.neo4jrest;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import com.tinkerpop.blueprints.pgm.impls.neo4jrest.util.Neo4jRestEdgeSequence;
import com.tinkerpop.blueprints.pgm.impls.neo4jrest.util.Neo4jRestVertexSequence;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.entity.RestRelationship;
import org.neo4j.rest.graphdb.query.RestGremlinQueryEngine;
import org.neo4j.rest.graphdb.util.QueryResult;


/**
 * A Blueprints implementation of the graph database Neo4j REST API (http://neo4j.org)
 *
 * @author Marcin Cieslik (marcin.cieslik@gmail.com)
 */
public class Neo4jRestGraph implements Graph {
	
	private RestGraphDatabase rawGraph;
	private RestGremlinQueryEngine gremlinQueryEngine;

	public Neo4jRestGraph(final RestGraphDatabase rawGraph) {
		this.rawGraph = rawGraph;
		this.gremlinQueryEngine = new RestGremlinQueryEngine(rawGraph.getRestAPI());
	}
	
	public Neo4jRestGraph(final String uri) {
		this(uri, null, null);
	}
	
	public Neo4jRestGraph(final String uri, String user, String password) {
		this(new RestGraphDatabase(uri, user, password));
		
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
}

