package com.tinkerpop.blueprints.pgm.impls.neo4jrest;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

import org.neo4j.graphdb.NotFoundException;

import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.entity.RestEntity;
import org.neo4j.rest.graphdb.entity.RestRelationship;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Marcin Cieslik (marcin.cieslik@gmail.com)
 */
public abstract class Neo4jRestElement implements Element {

    protected final Neo4jRestGraph graph;
    protected RestEntity rawElement;

    public Neo4jRestElement(final Neo4jRestGraph graph) {
        this.graph = graph;
    }

    public Object getProperty(final String key) {
        if (this.rawElement.hasProperty(key))
            return this.rawElement.getProperty(key);
        else
            return null;
    }

    public void setProperty(final String key, final Object value) {
        if (key.equals(StringFactory.ID) || (key.equals(StringFactory.LABEL) && this instanceof Edge))
            throw new RuntimeException(key + StringFactory.PROPERTY_EXCEPTION_MESSAGE);
        Object oldValue = this.getProperty(key); // ?
        for (Neo4jRestAutomaticIndex autoIndex : this.graph.getAutoIndices(this.getClass())) {
        	 autoIndex.autoUpdate(key, value, oldValue, this);
        }
        this.rawElement.setProperty(key, value);
    }

    public Object removeProperty(final String key) {
    	try {
    		Object oldValue = this.rawElement.removeProperty(key);
            if (null != oldValue) {
                for (Neo4jRestAutomaticIndex autoIndex : this.graph.getAutoIndices(this.getClass())) {
                    autoIndex.autoRemove(key, oldValue, this);
                }
            }
    		return oldValue;
    	} catch (NotFoundException e) {
    		return null;
    	}
    }

    public Set<String> getPropertyKeys() {
        final Set<String> keys = new HashSet<String>();
        for (final String key : this.rawElement.getPropertyKeys()) {
            keys.add(key);
        }
        return keys;
    }

    public int hashCode() {
        return this.getId().hashCode();
    }

    public RestEntity getRawElement() {
        return this.rawElement;
    }

    public Object getId() {
        if (this.rawElement instanceof RestNode) {
            return ((RestNode) this.rawElement).getId();
        } else {
            return ((RestRelationship) this.rawElement).getId();
        }
    }

    public boolean equals(final Object object) {
        return (null != object) && (this.getClass().equals(object.getClass()) && this.getId().equals(((Element) object).getId()));
    }
}
