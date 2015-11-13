package net.deelam.graphtools.graphfactories;

import java.io.IOException;

import net.deelam.graphtools.GraphUri;

import org.apache.commons.configuration.BaseConfiguration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

public class IdGraphFactoryOrientdbTest {

  @BeforeClass
  public static void setUp() throws Exception {
    IdGraphFactoryOrientdb.register();
  }

  @Test
  public void testOrientInMemDB() throws IOException {
    GraphUri gUri = new GraphUri("orientdb:memory:myODb");
    IdGraph<OrientGraph> graph = gUri.createNewIdGraph(true);
    graph.shutdown();
  }

  @Test
  public void testOrientAbsoluteLocalDB() throws IOException {
    GraphUri gUri = new GraphUri("orientdb:plocal:/tmp/target/myPLocalODb");
    IdGraph<OrientGraph> graph = gUri.createNewIdGraph(true);
    graph.shutdown();
  }

  @Test
  public void testOrientRelativeLocalDB() {
    GraphUri gUri = new GraphUri("orientdb:plocal:./target/myPLocalODb");
    IdGraph<OrientGraph> graph = gUri.openIdGraph();
    graph.shutdown();
  }

  @Test
  public void testOrientUsingConf() {
    BaseConfiguration conf = new BaseConfiguration();
    conf.setProperty("node_auto_indexing", "false"); // FIXME: insert Orient setting that can be checked

    GraphUri gUri = new GraphUri("orientdb:memory:myODb", conf);
    IdGraph<OrientGraph> graph = gUri.openIdGraph();
    //		OrientGraph neo = graph.getBaseGraph();
    //		AutoIndexer<Node> autoIndexer = neo.getRawGraph().index().getNodeAutoIndexer(); // TODO: check if config took effect
    //		System.out.println(autoIndexer);
    graph.shutdown();
  }

  @Test
  public void testOrientUsingUriQuery() {
    {
      GraphUri gUri = new GraphUri("orientdb:plocal:./target/myODb?username=admin&password=admin");
      IdGraph<OrientGraph> graph = gUri.openIdGraph();
      graph.shutdown();
    }
    {
      GraphUri gUri2 = new GraphUri("orientdb:plocal:./target/myODb?username=admin&password=admin");
      IdGraph<OrientGraph> graph2 = gUri2.openIdGraph();
      graph2.shutdown();
    }
  }

  @Test
  public void testOrientUsingUriQueryDiffPwd() throws IOException {
    {
      GraphUri gUri =
          new GraphUri("orientdb:plocal:./target/myOwnODb");
      IdGraph<OrientGraph> graph = gUri.createNewIdGraph(true);
      graph.shutdown();
    }
    {
      GraphUri gUri2 =
          new GraphUri("orientdb:plocal:./target/myOwnODb?username=reader&password=reader");
      IdGraph<OrientGraph> graph2 = gUri2.openIdGraph();
      graph2.shutdown();
    }
  }

  @Test
  public void testTwoOrientUsingUriQuery() {
    GraphUri gUri = new GraphUri("orientdb:plocal:./target/myODb");
    IdGraph<OrientGraph> graph = gUri.openIdGraph();

    GraphUri gUri2 = new GraphUri("orientdb:plocal:./target/myODb2");
    IdGraph<OrientGraph> graph2 = gUri2.openIdGraph();

    graph.shutdown();
    graph2.shutdown();
  }

  @Test
  public void testUriReuse() throws IOException {
    //FileUtils.deleteDirectory(new File("target/myDb2")); // make sure graph doesn't exist

    GraphUri gUri = new GraphUri("orientdb:plocal:./target/myODb2");
    IdGraph<OrientGraph> graph = gUri.openIdGraph();

    try {
      gUri.openIdGraph(); // should be able to connect to graph simultaneously
    } catch (RuntimeException re) {
      Assert.fail();
    }
    graph.shutdown();
  }

}
