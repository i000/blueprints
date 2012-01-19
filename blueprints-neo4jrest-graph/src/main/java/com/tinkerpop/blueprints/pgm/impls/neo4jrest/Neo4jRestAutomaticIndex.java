package com.tinkerpop.blueprints.pgm.impls.neo4jrest;

import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import org.neo4j.rest.graphdb.entity.RestEntity;
import org.neo4j.rest.graphdb.index.RestIndexManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Marcin Cieslik (marcin.cieslik@gmail.com)
 */
public class Neo4jRestAutomaticIndex<T extends Neo4jRestElement, S extends RestEntity> extends Neo4jRestAbstractIndex<T, S> implements AutomaticIndex<T> {

    private Set<String> autoIndexKeys;

    public Neo4jRestAutomaticIndex(final String indexName, final Class<T> indexClass, final Neo4jRestGraph graph, final Set<String> keys) {
        super(indexName, indexClass, graph);
        this.generateIndex(keys);	
    }

    public Type getIndexType() {
        return Type.AUTOMATIC;
    }

    protected void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
        if (null == this.autoIndexKeys || this.autoIndexKeys.contains(key)) {
            if (oldValue != null)
                this.removeBasic(key, oldValue, element);
            this.putBasic(key, newValue, element);
        }
    }

    protected void autoRemove(final String key, final Object oldValue, final T element) {
        if (null == this.autoIndexKeys || this.autoIndexKeys.contains(key)) {
            this.removeBasic(key, oldValue, element);
        }
    }

    public Set<String> getAutoIndexKeys() {
        return this.autoIndexKeys;
    }

    private void generateIndex(final Set<String> keys) {
    	
    	RestIndexManager manager = graph.getRawGraph().index();
    	boolean vertices = this.indexClass.isAssignableFrom(Neo4jRestVertex.class);
    	boolean exists = vertices ? manager.existsForNodes(this.indexName) : manager.existsForRelationships(this.indexName);

    	if (!exists) {
    		// copy keys
    	    if (null != keys) {
    	    	this.autoIndexKeys = new HashSet<String>();
    	    	this.autoIndexKeys.addAll(keys);
    	    }
    	    // keys to string
    	    String field;
    	    if (null != this.autoIndexKeys) {
    	        field = "";
    	        for (final String key : this.autoIndexKeys) {
    	            field = field + Neo4jRestTokens.KEY_SEPARATOR + key;
    	        }
    	    } else {
    	        field = "null";
    	    }
    		
    		HashMap<String, String> config = new HashMap<String, String>();
    		config.put(Neo4jRestTokens.BLUEPRINTS_TYPE, Type.AUTOMATIC.toString());
    		config.put(Neo4jRestTokens.BLUEPRINTS_AUTOKEYS, field);
    		this.rawIndex = vertices ? (org.neo4j.rest.graphdb.index.RestIndex<S>) manager.forNodes(this.indexName, config) :
    								   (org.neo4j.rest.graphdb.index.RestIndex<S>) manager.forRelationships(this.indexName, config);
    	}	
    	else {
    		// index exists
            this.rawIndex = vertices ? (org.neo4j.rest.graphdb.index.RestIndex<S>) manager.forNodes(this.indexName) : 
            	                       (org.neo4j.rest.graphdb.index.RestIndex<S>) manager.forRelationships(this.indexName);
            // string to keys
            final String keysString = this.getIndexManager().getConfiguration(this.rawIndex).get(Neo4jRestTokens.BLUEPRINTS_AUTOKEYS);
            if (keysString.equals("null")) {
            	this.autoIndexKeys = null;
            }
            else {
        	    this.autoIndexKeys = new HashSet<String>();
                for (final String key : keysString.split(Neo4jRestTokens.KEY_SEPARATOR)) {
            	    if (key.length() > 0) {
            		    this.autoIndexKeys.add(key);
            	    }
                }
            }
            
            final String storedType = manager.getConfiguration(this.rawIndex).get(Neo4jRestTokens.BLUEPRINTS_TYPE);
            if (null == storedType || this.getIndexType() != Type.valueOf(storedType)) {
                throw new RuntimeException("Stored index is " + storedType + " and is being loaded as a " + this.getIndexType() + " index");            	
            }
    	}	
    }
    
}