package com.tinkerpop.blueprints.pgm.impls.neo4jrest;

//import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.entity.RestRelationship;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jRestEdge extends Neo4jRestElement implements Edge {

    public Neo4jRestEdge(final RestRelationship relationship, final Neo4jRestGraph graph) {
        this(relationship, graph, false);
    }

    protected Neo4jRestEdge(final RestRelationship relationship, final Neo4jRestGraph graph, boolean isNew) {
        super(graph);
        this.rawElement = relationship;
//        if (isNew) {
//            for (final Neo4jAutomaticIndex autoIndex : this.graph.getAutoIndices(Neo4jRestEdge.class)) {
//                autoIndex.autoUpdate(AutomaticIndex.LABEL, this.getLabel(), null, this);
//            }
//        }
    }

    public String getLabel() {
        return ((RestRelationship) this.rawElement).getType().name();
    }

    public Vertex getOutVertex() {
        return new Neo4jRestVertex((RestNode) ((RestRelationship) this.rawElement).getStartNode(), this.graph);
    }

    public Vertex getInVertex() {
        return new Neo4jRestVertex((RestNode) ((RestRelationship) this.rawElement).getEndNode(), this.graph);
    }

    public boolean equals(final Object object) {
        return object instanceof Neo4jRestEdge && ((Neo4jRestEdge) object).getId().equals(this.getId());
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    public RestRelationship getRawEdge() {
        return (RestRelationship) this.rawElement;
    }
}
