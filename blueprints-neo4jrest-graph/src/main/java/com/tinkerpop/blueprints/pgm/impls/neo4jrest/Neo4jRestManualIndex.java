package com.tinkerpop.blueprints.pgm.impls.neo4jrest;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;
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
public class Neo4jRestManualIndex<T extends Neo4jRestElement, S extends RestEntity> extends Neo4jRestAbstractIndex<T, S> {

    public Neo4jRestManualIndex(final String indexName, final Class<T> indexClass, final Neo4jRestGraph graph) {
    	super(indexName, indexClass, graph);
        this.generateIndex();
    }

    public Type getIndexType() {
        return Type.MANUAL;
    }

    private void generateIndex() {
    	
    	RestIndexManager manager = graph.getRawGraph().index();
    	boolean vertices = this.indexClass.isAssignableFrom(Neo4jRestVertex.class);
    	boolean exists = vertices ? manager.existsForNodes(this.indexName) : manager.existsForRelationships(this.indexName);

    	if (!exists) {
    		// index does not exist make new one
    		HashMap<String, String> config = new HashMap<String, String>();
    		config.put(Neo4jRestTokens.BLUEPRINTS_TYPE, Type.MANUAL.toString());
    		this.rawIndex = vertices ? (org.neo4j.rest.graphdb.index.RestIndex<S>) manager.forNodes(this.indexName, config) :
    								   (org.neo4j.rest.graphdb.index.RestIndex<S>) manager.forRelationships(this.indexName, config);
    	}	
    	else {
    		// index exists
            this.rawIndex = vertices ? (org.neo4j.rest.graphdb.index.RestIndex<S>) manager.forNodes(this.indexName) : 
            	                       (org.neo4j.rest.graphdb.index.RestIndex<S>) manager.forRelationships(this.indexName);
            
            
            final String storedType = manager.getConfiguration(this.rawIndex).get(Neo4jRestTokens.BLUEPRINTS_TYPE);
            if (null == storedType || this.getIndexType() != Type.valueOf(storedType)) {
                throw new RuntimeException("Stored index is " + storedType + " and is being loaded as a " + this.getIndexType() + " index");            	
            }
    	}	
    }
}
