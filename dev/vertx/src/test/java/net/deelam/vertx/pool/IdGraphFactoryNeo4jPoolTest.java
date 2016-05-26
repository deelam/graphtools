package net.deelam.vertx.pool;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;

@RunWith(VertxUnitRunner.class)
@Slf4j
public class IdGraphFactoryNeo4jPoolTest {
  private Vertx vertx;

  @Before
  public void setUp(final TestContext context) throws Exception {
    vertx = Vertx.vertx();
    Async async = context.async(1);
    Handler<AsyncResult<String>> deployHandler = res -> {
      async.countDown();
    };

    Injector injector = Guice.createInjector(new AbstractModule() {
      private String pondId = "poolTest";
      
      @Override
      protected void configure() {
        PondVerticle pond = new PondVerticle(pondId);

        PondVerticle.Serializer serializer = SerDeserUtils.createGraphUriSerializer();
        PondVerticle.Deserializer deserializer = SerDeserUtils.createGraphUriDeserializer();
        pond.register("neo4j", serializer, deserializer);
        vertx.deployVerticle(pond, deployHandler);
      }

      @Provides
      ResourcePoolClient provideResourcePoolClient() {
        ResourcePoolClient client = new ResourcePoolClient(pondId);
        vertx.deployVerticle(client);
        return client;
      }
    });

    IdGraphFactoryNeo4jPool.register(injector.getInstance(ResourcePoolClient.class));
  }

  @After
  public void tearDown(final TestContext context) throws Exception {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void testSequentialReadOnly(final TestContext context) throws IOException, InterruptedException {
    GraphUri gUri1 = new GraphUri("neo4j:///./target/myDb");
    IdGraph<Neo4jGraph> graph1 = gUri1.createNewIdGraph(true);
    gUri1.shutdown();

    GraphUri gUri2 = new GraphUri("neo4j:///./target/myDb").readOnly();
    IdGraph<Neo4jGraph> graph2 = gUri2.openIdGraph();
    String graph2Str = graph2.toString();
    gUri2.shutdown();

    GraphUri gUri3 = new GraphUri("neo4j:///./target/myDb").readOnly();
    IdGraph<Neo4jGraph> graph3 = gUri3.openIdGraph();
    assertEquals(graph2Str, graph3.toString());
    gUri3.shutdown();
    
    gUri1.delete();
  }
  
  @Test
  public void testParallelReadOnly(final TestContext context) throws IOException, InterruptedException {
    GraphUri gUri1 = new GraphUri("neo4j:///./target/myDb");
    IdGraph<Neo4jGraph> graph1 = gUri1.createNewIdGraph(true);
    gUri1.shutdown();

    GraphUri gUri2 = new GraphUri("neo4j:///./target/myDb").readOnly();
    IdGraph<Neo4jGraph> graph2 = gUri2.openIdGraph();
    String graph2Str = graph2.toString();

    GraphUri gUri3 = new GraphUri("neo4j:///./target/myDb").readOnly();
    IdGraph<Neo4jGraph> graph3 = gUri3.openIdGraph();
    assertNotSame(graph2Str, graph3.toString());
    
    gUri2.shutdown();
    gUri3.shutdown();

    gUri1.delete();
  }

}
