package com.tinkerpop.blueprints.pgm.impls.neo4jrest;


import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.MultiIterable;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import com.tinkerpop.blueprints.pgm.impls.neo4jrest.util.Neo4jRestEdgeSequence;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.rest.graphdb.entity.RestNode;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jRestVertex extends Neo4jRestElement implements Vertex {

    public Neo4jRestVertex(final RestNode node, final Neo4jRestGraph graph) {
        super(graph);
        this.rawElement = node;

    }

    public Iterable<Edge> getInEdges(final String... labels) {
        if (labels.length == 0)
            return new Neo4jRestEdgeSequence(((RestNode) this.rawElement).getRelationships(Direction.INCOMING), this.graph);
        else if (labels.length == 1) {
            return new Neo4jRestEdgeSequence(((RestNode) this.rawElement).getRelationships(DynamicRelationshipType.withName(labels[0]), Direction.INCOMING), this.graph);
        } else {
            final List<Iterable<Edge>> edges = new ArrayList<Iterable<Edge>>();
            for (final String label : labels) {
                edges.add(new Neo4jRestEdgeSequence(((RestNode) this.rawElement).getRelationships(DynamicRelationshipType.withName(label), Direction.INCOMING), this.graph));
            }
            return new MultiIterable<Edge>(edges);
        }
    }

    public Iterable<Edge> getOutEdges(final String... labels) {
        if (labels.length == 0)
            return new Neo4jRestEdgeSequence(((RestNode) this.rawElement).getRelationships(Direction.OUTGOING), this.graph);
        else if (labels.length == 1) {
            return new Neo4jRestEdgeSequence(((RestNode) this.rawElement).getRelationships(DynamicRelationshipType.withName(labels[0]), Direction.OUTGOING), this.graph);
        } else {
            final List<Iterable<Edge>> edges = new ArrayList<Iterable<Edge>>();
            for (final String label : labels) {
                edges.add(new Neo4jRestEdgeSequence(((RestNode) this.rawElement).getRelationships(DynamicRelationshipType.withName(label), Direction.OUTGOING), this.graph));
            }
            return new MultiIterable<Edge>(edges);
        }
    }


    public boolean equals(final Object object) {
        return object instanceof Neo4jRestVertex && ((Neo4jRestVertex) object).getId().equals(this.getId());
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public RestNode getRawVertex() {
        return (RestNode) this.rawElement;
    }

}
