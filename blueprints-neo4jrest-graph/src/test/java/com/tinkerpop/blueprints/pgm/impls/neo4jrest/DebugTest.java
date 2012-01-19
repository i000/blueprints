package com.tinkerpop.blueprints.pgm.impls.neo4jrest;

import static org.junit.Assert.*;

import java.util.Iterator;

import org.junit.After;
import org.junit.Before;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.rest.graphdb.index.RestIndexManager;
import org.neo4j.rest.graphdb.query.RestGremlinQueryEngine;
import org.neo4j.rest.graphdb.util.QueryResult;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;

// rough test
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;

public class DebugTest {

	private String server = "http://127.0.0.1:7474/db/data";
	private Neo4jRestGraph graph;
	private Neo4jGraph embed;

	@Before
	public void setUp() throws Exception {
		this.graph = new Neo4jRestGraph(server);
		this.embed = new Neo4jGraph("/tmp/newneo");
	}

	@After
	public void tearDown() throws Exception {
		this.graph.shutdown();
		this.embed.shutdown();
	}

	@Test
	public void testDebug() {
		System.out.println(this.graph);
		System.out.println(this.embed);


		
		IndexManager index = this.graph.getRawGraph().index();
		System.out.println(index.nodeIndexNames().length + index.relationshipIndexNames().length);
		
		// vertices and edges
//        final RestIndexManager manager = this.graph.getRawGraph().index();
//          for (final String indexName : manager.relationshipIndexNames()) {
//        	System.out.println(indexName);
//        }
//  		for (Index idx : this.graph.getIndices()) {
//			this.graph.dropIndex(idx.getIndexName());
//		}

		Iterable<Index<? extends Element>> gi = this.graph.getIndices();
		Iterable<Index<? extends Element>> ei = this.embed.getIndices();
		System.out.println(gi);
		System.out.println(ei);
		
		
		// {"_blueprints:type":"AUTOMATIC", "_blueprints:autokeys":"null",
		// "type":"exact"}
		// this.graph.clear();
		// System.out.println(this.graph);
		// Vertex v0 = this.graph.getVertex(0);
		// System.out.println(v0);
		// this.graph.clear();

		// Neo4jRestVertex v1 = (Neo4jRestVertex) this.graph.addVertex(null);
		// Object id1 = v1.getId();
		// Neo4jRestVertex v2 = (Neo4jRestVertex) this.graph.getVertex(id1);
		// assertEquals(v1, v2);
		// Vertex v3 = this.graph.addVertex(null);
		// assertFalse(v1.equals(v3));
		// Object id3 = v3.getId();
		// graph.removeVertex(v3);
		// assertNull(graph.getVertex(id3));
		// assertNotNull(graph.getVertex(id1));
		// Vertex v4 = this.graph.addVertex(null);
		// Edge e1 = (Neo4jRestEdge) this.graph.addEdge(null, v1, v4, "links");
		// Object eid1 = e1.getId();
		// System.out.println(eid1);
		// Edge e2 = this.graph.getEdge(eid1);
		// assertEquals(e1, e2);
		// this.graph.removeEdge(e1);
		// assertNull(graph.getEdge(eid1));
		// Edge e3 = (Neo4jRestEdge) this.graph.addEdge(null, v4, v1, "links");
		// RestGremlinQueryEngine gr = new
		// RestGremlinQueryEngine(graph.getRawGraph().getRestAPI());
		// QueryResult<Object> query = gr.query("g.V", null);
		// for (Object obj : query) {
		// //System.out.println(obj);
		// }
		// for (Edge e : this.graph.getEdges()) {
		// System.out.println(e);
		// }
		// //this.graph.clear();
		//
		// // Iterable<Vertex> vs = this.graph.getVertices();
		// // for (Vertex v: vs) {
		// // //System.out.println(v);
		// // }
		//
		//
		// //asse(v1.equals(v2));
		// //assertSame(v1, v2);
		// System.out.println(v1.getId());
		// System.out.println(e3.getId());
		//
		// Neo4jRestVertex v5 = (Neo4jRestVertex) this.graph.addVertex(null);
		// Neo4jRestVertex v6 = (Neo4jRestVertex) this.graph.addVertex(null);
		// Neo4jRestEdge e4 = (Neo4jRestEdge) this.graph.addEdge(null, v5, v6,
		// "links");
		//
		// System.out.println(v5);
		// this.graph.removeVertex(v5);
		// System.out.println(this.graph.getVertex(v5.getId()));
		//

	}

}
