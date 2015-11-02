package graphtools.importer;

import graphtools.GraphUri;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

public class GraphUriTest {

	@Before
	public void setUp() throws Exception {
//		new File("testGraphs").mkdir();
	}

	@AfterClass
	public static void tearDown() throws IOException{
//		FileUtils.deleteDirectory(new File("testGraphs"));
	}

	@Test
	public void testInMemGraph() {
		GraphUri gUri = new GraphUri("tinker:///");
		IdGraph<TinkerGraph> graph = gUri.openIdGraph();
		graph.shutdown();
	}
	
	@Test
	public void testSavedGraph() {
		GraphUri gUri = new GraphUri("tinker:///./target/tGraph");
		IdGraph<TinkerGraph> graph = gUri.openIdGraph();
		graph.shutdown();
	}

	@Test
	public void testSavedGraphml() {
		GraphUri gUri = new GraphUri("tinker:///./target/tGraphML?fileType=graphml");
		IdGraph<TinkerGraph> graph = gUri.openIdGraph();
		graph.shutdown();
	}
}
