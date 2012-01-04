package com.tinkerpop.blueprints.pgm.impls.neo4jrest.util;


import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.impls.neo4jrest.Neo4jRestEdge;
import com.tinkerpop.blueprints.pgm.impls.neo4jrest.Neo4jRestGraph;
import org.neo4j.graphdb.Relationship;
import org.neo4j.rest.graphdb.entity.RestRelationship;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jRestEdgeSequence<T extends Edge> implements CloseableSequence<Neo4jRestEdge> {

    private final Iterator<RestRelationship> relationships;
    private final Neo4jRestGraph graph;

    public Neo4jRestEdgeSequence(final Iterable<RestRelationship> relationships, final Neo4jRestGraph graph) {
        this.graph = graph;
        this.relationships = relationships.iterator();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public Neo4jRestEdge next() {
        return new Neo4jRestEdge(this.relationships.next(), this.graph);
    }

    public boolean hasNext() {
        return this.relationships.hasNext();
    }

    public Iterator<Neo4jRestEdge> iterator() {
        return this;
    }

    public void close() {
//        if (this.relationships instanceof IndexHits) {
//            ((IndexHits) this.relationships).close();
//        }
    }
}