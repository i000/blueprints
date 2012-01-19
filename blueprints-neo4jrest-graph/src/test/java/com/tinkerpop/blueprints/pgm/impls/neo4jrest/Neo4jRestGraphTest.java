package com.tinkerpop.blueprints.pgm.impls.neo4jrest;

import com.tinkerpop.blueprints.pgm.AutomaticIndexTestSuite;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.EdgeTestSuite;
import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.GraphTestSuite;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.IndexTestSuite;
import com.tinkerpop.blueprints.pgm.IndexableGraph;
import com.tinkerpop.blueprints.pgm.IndexableGraphTestSuite;
import com.tinkerpop.blueprints.pgm.TestSuite;
import com.tinkerpop.blueprints.pgm.TransactionalGraphTestSuite;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.VertexTestSuite;
import com.tinkerpop.blueprints.pgm.impls.GraphTest;
import com.tinkerpop.blueprints.pgm.util.graphml.GraphMLReaderTestSuite;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jRestGraphTest extends GraphTest {

	private String server = "http://127.0.0.1:7474/db/data";
	private Neo4jRestGraph graph;

    public Neo4jRestGraphTest() {
        this.allowsDuplicateEdges = true;
        this.allowsSelfLoops = false;
        this.isPersistent = true;
        this.isRDFModel = false;
        this.supportsVertexIteration = true;
        this.supportsEdgeIteration = true;
        this.supportsVertexIndex = true;
        this.supportsEdgeIndex = true;
        this.ignoresSuppliedIds = true;
        this.supportsTransactions = false;
    }

    /*public void testNeo4jBenchmarkTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new Neo4jBenchmarkTestSuite(this));
        printTestPerformance("Neo4jBenchmarkTestSuite", this.stopWatch());
    }*/

    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this));
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }

//  public void testQueryIndex() throws Exception {
//  String directory = System.getProperty("neo4jGraphDirectory");
//  if (directory == null)
//      directory = this.getWorkingDirectory();
//  IndexableGraph graph = new Neo4jGraph(directory);
//  Vertex a = graph.addVertex(null);
//  a.setProperty("name", "marko");
//  Iterator itty = graph.getIndex(Index.VERTICES, Vertex.class).get("name", Neo4jTokens.QUERY_HEADER + "*rko").iterator();
//  int counter = 0;
//  while (itty.hasNext()) {
//      counter++;
//      assertEquals(itty.next(), a);
//  }
//  assertEquals(counter, 1);
//
//  Vertex b = graph.addVertex(null);
//  Edge edge = graph.addEdge(null, a, b, "knows");
//  edge.setProperty("weight", 0.75);
//  itty = graph.getIndex(Index.EDGES, Edge.class).get("label", Neo4jTokens.QUERY_HEADER + "k?ows").iterator();
//  counter = 0;
//  while (itty.hasNext()) {
//      counter++;
//      assertEquals(itty.next(), edge);
//  }
//  assertEquals(counter, 1);
//  itty = graph.getIndex(Index.EDGES, Edge.class).get("weight", Neo4jTokens.QUERY_HEADER + "[0.5 TO 1.0]").iterator();
//  counter = 0;
//  while (itty.hasNext()) {
//      counter++;
//      assertEquals(itty.next(), edge);
//  }
//  assertEquals(counter, 1);
//  assertEquals(count(graph.getIndex(Index.EDGES, Edge.class).get("weight", Neo4jTokens.QUERY_HEADER + "[0.1 TO 0.5]")), 0);
//
//
//  graph.shutdown();
//  deleteDirectory(new File(directory));
//}
    
    public void testIndexableGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new IndexableGraphTestSuite(this));
        printTestPerformance("IndexableGraphTestSuite", this.stopWatch());
    }

    public void testIndexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new IndexTestSuite(this));
        printTestPerformance("IndexTestSuite", this.stopWatch());
    }

    public void testAutomaticIndexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new AutomaticIndexTestSuite(this));
        printTestPerformance("AutomaticIndexTestSuite", this.stopWatch());
    }


    public void testGraphMLReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphMLReaderTestSuite(this));
        printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
    }

    public Graph getGraphInstance() {
    	Neo4jRestGraph graph = new Neo4jRestGraph(server, false);
    	return graph;
    }
    
    public void doTestSuite(final TestSuite testSuite) throws Exception {
        String doTest = System.getProperty("testNeo4jRestGraph");
        if (doTest == null || doTest.equals("true")) {
            for (Method method : testSuite.getClass().getDeclaredMethods()) {
            	Neo4jRestGraph graph = (Neo4jRestGraph) this.getGraphInstance();
    
            	graph.clear();
          		for (Index idx : graph.getIndices()) {
        			graph.dropIndex(idx.getIndexName());
        		}
            	graph.shutdown();
            	graph = new Neo4jRestGraph(server);
  	
                if (method.getName().startsWith("test")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                    
                graph = (Neo4jRestGraph) this.getGraphInstance();
                    
              	for (Index idx : graph.getIndices()) {
            		graph.dropIndex(idx.getIndexName());
            		}
              	System.out.println(graph.getIndices());
              	graph.shutdown();
                graph.clear();
                }
            }
        }
    }
    
//    public static void main(String args[]) {
//        org.junit.runner.JUnitCore.main("com.tinkerpop.blueprints.pgm.impls.neo4jrest.Neo4jRestGraphTest");
//    }

}
