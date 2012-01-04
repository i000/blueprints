package com.tinkerpop.blueprints.pgm.impls.neo4jrest.util;


import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.neo4jrest.Neo4jRestGraph;
import com.tinkerpop.blueprints.pgm.impls.neo4jrest.Neo4jRestVertex;
import org.neo4j.rest.graphdb.entity.RestNode;
//import org.neo4j.graphdb.index.IndexHits;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * 
 */
public class Neo4jRestVertexSequence<T extends Vertex> implements CloseableSequence<Neo4jRestVertex> {

    private final Iterator<RestNode> nodes;
    private final Neo4jRestGraph graph;

    public Neo4jRestVertexSequence(final Iterable<RestNode> nodes, final Neo4jRestGraph graph) {
        this.graph = graph;
        this.nodes = nodes.iterator();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public Neo4jRestVertex next() {
        return new Neo4jRestVertex(this.nodes.next(), this.graph);
    }

    public boolean hasNext() {
        return this.nodes.hasNext();
    }

    public Iterator<Neo4jRestVertex> iterator() {
        return this;
    }

    public void close() {
//        if (this.nodes instanceof IndexHits) {
//            ((IndexHits) this.nodes).close();
//        }
    }
}