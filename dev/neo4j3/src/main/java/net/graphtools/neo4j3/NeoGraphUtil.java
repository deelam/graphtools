package net.graphtools.neo4j3;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.slf4j.Slf4jLogProvider;
import org.neo4j.tinkerpop.api.Neo4jGraphAPI;
import org.neo4j.tinkerpop.api.impl.Neo4jGraphAPIImpl;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NeoGraphUtil {

  public static Graph openGraph(File dbPath){
    Neo4jGraphAPI neoGraph=NeoGraphUtil.openNeoGraph(dbPath);
    return Neo4jGraph.open(neoGraph);
  }
  public static Neo4jGraphAPI openNeoGraph(File dbPath) {
    GraphDatabaseService graphDb = openGraphDB(dbPath);
    return new Neo4jGraphAPIImpl(graphDb);
  }

  public static GraphDatabaseService openGraphDB(File dbPath) {
    log.info("Opening " + dbPath.getAbsolutePath());
    LogProvider logProvider = new Slf4jLogProvider();
    //GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
    GraphDatabaseService graphDb = new GraphDatabaseFactory()
        .setUserLogProvider(logProvider)
        .newEmbeddedDatabaseBuilder(dbPath)
        //        .loadPropertiesFromFile( pathToConfig + "neo4j.conf" )
        //Cannot be readonly, so gremlin can setGraphProperty:         .setConfig(GraphDatabaseSettings.read_only, "true")
        //        .setConfig( GraphDatabaseSettings.pagecache_memory, "512M" )
        //        .setConfig( GraphDatabaseSettings.string_block_size, "60" )
        //        .setConfig( GraphDatabaseSettings.array_block_size, "300" )
        .newGraphDatabase();
    log.info("Opened graph: {}", graphDb);

    registerShutdownHook(graphDb);
    return graphDb;
  }

  private static void registerShutdownHook(final GraphDatabaseService graphDb) {
    // Registers a shutdown hook for the Neo4j instance so that it
    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
    // running application).
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        log.info("Shutting down... "+graphDb);
        graphDb.shutdown();
        log.info("Shutdown "+graphDb);
      }
    });
  }

  void createIndex(Neo4jGraphAPI graphAPI) {
    GraphDatabaseService graphDb = ((Neo4jGraphAPIImpl) graphAPI).getGraphDatabase();
    try (Transaction tx = graphDb.beginTx()) {
      Schema schema = graphDb.schema();
      IndexDefinition indexDefinition = schema.indexFor(Label.label("Crime"))
          .on("id")
          .create();
      tx.success();

      System.out.println(String.format("Percent complete: %1.0f%%",
          schema.getIndexPopulationProgress(indexDefinition).getCompletedPercentage()));

      // Indexes are populated asynchronously when they are first created. 
      // If desired, can wait for index population to complete:
      schema.awaitIndexOnline(indexDefinition, 10, TimeUnit.SECONDS);

      System.out.println(String.format("Percent complete: %1.0f%%",
          schema.getIndexPopulationProgress(indexDefinition).getCompletedPercentage()));
    }

    try (Transaction tx = graphDb.beginTx()) {
      Schema schema = graphDb.schema();
      System.out.println("Indexes: " + schema.getIndexes());
    }
    
    {
      int i=0;
      Transaction tx = graphDb.beginTx();
      for(Node n:graphDb.getAllNodes()){
        System.out.println(n+" "+n.getAllProperties()+" "+n.getProperty("id").getClass());
        
        if(++i>10)
          break;
      }
      tx.success();
    }
    
  }

}
