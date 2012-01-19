package com.tinkerpop.blueprints.pgm.impls.neo4jrest;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import com.tinkerpop.blueprints.pgm.impls.neo4jrest.util.Neo4jRestEdgeSequence;
import com.tinkerpop.blueprints.pgm.impls.neo4jrest.util.Neo4jRestVertexSequence;

import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.entity.RestEntity;
import org.neo4j.rest.graphdb.entity.RestRelationship;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.index.SimpleIndexHits;
import org.neo4j.rest.graphdb.index.RestIndexManager;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Marcin Cieslik (marcin.cieslik@gmail.com)
 */
public abstract class Neo4jRestAbstractIndex<T extends Neo4jRestElement, S extends RestEntity> implements Index<T> {
	
    protected final Class<T> indexClass;
    protected final Neo4jRestGraph graph;
    protected final String indexName;
    protected RestIndex<S> rawIndex;

    public Neo4jRestAbstractIndex(final String indexName, final Class<T> indexClass, final Neo4jRestGraph graph) {
        this.indexClass = indexClass;
        this.graph = graph;
        this.indexName = indexName;
    }
      
    public Class<T> getIndexClass() {
        if (Vertex.class.isAssignableFrom(this.indexClass))
            return (Class) Vertex.class;
        else
            return (Class) Edge.class;
    }
    
    public String getIndexName() {
        return this.indexName;
    }

    public void put(final String key, final Object value, final T element) {
        try {
            this.rawIndex.add((S) element.getRawElement(), key, value);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public CloseableSequence<T> get(final String key, final Object value) {
        final SimpleIndexHits<S> itty;
        if (value instanceof String && ((String) value).startsWith(Neo4jRestTokens.QUERY_HEADER)) {
            itty = (SimpleIndexHits<S>) this.rawIndex.query(key, ((String) value).substring(Neo4jRestTokens.QUERY_HEADER.length()));
        } else {
            itty = (SimpleIndexHits<S>) this.rawIndex.get(key, value);
        }
        if (this.indexClass.isAssignableFrom(Neo4jRestVertex.class))
            return new Neo4jRestVertexSequence((Iterable<RestNode>) itty, this.graph);
        else
            return new Neo4jRestEdgeSequence((Iterable<RestRelationship>) itty, this.graph);
    }    

    public long count(final String key, final Object value) {
        return this.rawIndex.get(key, value).size();
    }

    public void remove(final String key, final Object value, final T element) {
        try {
            this.rawIndex.remove((S) element.getRawElement(), key, value);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }	
    
    protected void removeBasic(final String key, final Object value, final T element) {
        this.rawIndex.remove((S) element.getRawElement(), key, value);
    }

    protected void putBasic(final String key, final Object value, final T element) {
        this.rawIndex.add((S) element.getRawElement(), key, value);
    }
    
    protected RestIndexManager getIndexManager() {
        return this.graph.getRawGraph().index();
    }
	
    public String toString() {
        return StringFactory.indexString(this);
    }

}
