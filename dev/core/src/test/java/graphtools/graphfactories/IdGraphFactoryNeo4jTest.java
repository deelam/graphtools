package graphtools.graphfactories;

import java.io.File;
import java.io.IOException;

import graphtools.GraphUri;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

public class IdGraphFactoryNeo4jTest {

	@BeforeClass
	public static void setUp() throws Exception {
		IdGraphFactoryNeo4j.register();
	}

	@Test
	public void testNeo4j() {		
		GraphUri gUri = new GraphUri("neo4j:///./target/myDb");
		IdGraph<TinkerGraph> graph = gUri.openIdGraph();
		graph.shutdown();
	}

	@Test
	public void testNeo4jUsingConf() {
		BaseConfiguration conf=new BaseConfiguration();
		conf.setProperty("node_auto_indexing", "false"); // FIXME: insert neo4j setting that can be checked
		
		GraphUri gUri = new GraphUri("neo4j:///./target/myDb", conf);
		IdGraph<Neo4jGraph> graph = gUri.openIdGraph();
//		Neo4jGraph neo = graph.getBaseGraph();
//		AutoIndexer<Node> autoIndexer = neo.getRawGraph().index().getNodeAutoIndexer(); // TODO: check if config took effect
//		System.out.println(autoIndexer);
		graph.shutdown();
	}

	@Test
	public void testNeo4jUsingUriQuery() {
		
		GraphUri gUri = new GraphUri("neo4j:///./target/myDb?node_auto_indexing=true");
		IdGraph<Neo4jGraph> graph = gUri.openIdGraph();
		graph.shutdown();
	}

	@Test
	public void testTwoNeo4jUsingUriQuery() {
		GraphUri gUri = new GraphUri("neo4j:///./target/myDb?node_auto_indexing=true");
		IdGraph<Neo4jGraph> graph = gUri.openIdGraph();

		GraphUri gUri2 = new GraphUri("neo4j:///./target/myDb2?node_auto_indexing=true");
		IdGraph<Neo4jGraph> graph2 = gUri2.openIdGraph();
		
		graph.shutdown();
		graph2.shutdown();
	}
	
	@Test
	public void testUriReuse() throws IOException {
		//FileUtils.deleteDirectory(new File("target/myDb2")); // make sure graph doesn't exist
		
		GraphUri gUri = new GraphUri("neo4j:///./target/myDb2?fileType=graphml");
		IdGraph<TinkerGraph> graph = gUri.openIdGraph();
		
		try{
			gUri.openIdGraph(); // should not be able to connect to graph simultaneously
			Assert.fail();
		}catch(RuntimeException re){
		}
		graph.shutdown();
	}

}
