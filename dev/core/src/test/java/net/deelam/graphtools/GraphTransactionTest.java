package net.deelam.graphtools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.collection.Iterables;

import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

@Slf4j
public class GraphTransactionTest {

  protected static TransactionalGraph outGraph;

  @Before
  public void setUp() throws Exception {
    GraphTransaction.checkTransactionsClosed();
    //System.out.println("Resetting");
    FileUtils.deleteDirectory(new File("target/transGraph"));
    counter = 0;
    outGraph = new IdGraph<Neo4jGraph>(new Neo4jGraph("target/transGraph"));
    outGraph.addVertex("1");
    System.out.println(GraphUtils.count(outGraph));
    //cleanFiles();
  }

  void shutdownGraphs() {
    outGraph.shutdown();
  }

  @After
  public void cleanFiles() throws IOException {
    outGraph.shutdown();
    FileUtils.deleteDirectory(new File("target/transGraph"));
  }

  static private int counter = 1;

  private Integer proc() {
    outGraph.addVertex("_" + (++counter));
    //System.out.println(counter);
    return counter;
  }

  @Test
  public void testExecuteTransaction() {
    int tx = GraphTransaction.begin(outGraph);
    proc();
    GraphTransaction.commit(tx);
    assertEquals(2, Iterables.count(outGraph.getVertices()));
    //System.out.println(GraphUtils.toString(outGraph));
  }

  @Test
  public void testExecuteTransactionSerially() {
    int tx = GraphTransaction.begin(outGraph);
    proc();
    GraphTransaction.commit(tx);
    assertEquals(2, Iterables.count(outGraph.getVertices()));

    tx = GraphTransaction.begin(outGraph);
    proc();
    GraphTransaction.rollback(tx);
    assertEquals(2, Iterables.count(outGraph.getVertices()));

    tx = GraphTransaction.begin(outGraph);
    proc();
    GraphTransaction.commit(tx);
    assertEquals(3, Iterables.count(outGraph.getVertices()));
  }

  @Test
  public void testNestedExecuteTransaction() {
    int tx = GraphTransaction.begin(outGraph);
    testExecuteTransaction();
    proc();
    GraphTransaction.commit(tx);
    assertEquals(3, Iterables.count(outGraph.getVertices()));
  }

  @SuppressWarnings("serial")
  static class ExpectedException extends RuntimeException {
  }

  public void testExecuteTransactionFailure() {
    int tx = GraphTransaction.begin(outGraph);
    proc();
    if (counter == 1) {
      GraphTransaction.rollback(tx);
      throw new ExpectedException();
    }
    GraphTransaction.commit(tx);
  }

  @Test
  public void testNestedExecuteTransactionFailure() throws Exception {
    assertEquals(1, Iterables.count(outGraph.getVertices()));
    int tx = GraphTransaction.begin(outGraph);
    try {
      testExecuteTransactionFailure();
      fail("Exception expected");
      GraphTransaction.commit(tx);
    } catch (ExpectedException re) {
      // expect exception be thrown
      log.info("vertex1={}", outGraph.getVertex("_" + 1));
      GraphTransaction.rollback(tx);
      assertEquals(1, Iterables.count(outGraph.getVertices()));
    }
  }

  public void testExecuteTransactionFailureNoExplicitRollback() {
    int tx = GraphTransaction.begin(outGraph);
    proc();
    if (counter == 1) {
      throw new RuntimeException(
          "unexpected and uncaught exception, such that rollback() is not called");
    }
    GraphTransaction.commit(tx);
  }

  @Test
  public void testNestedExecuteTransactionFailureNoNestedRollback() throws Exception {
    assertEquals(1, Iterables.count(outGraph.getVertices()));
    int tx = GraphTransaction.begin(outGraph);
    try {
      testExecuteTransactionFailureNoExplicitRollback();
      fail("RuntimeException expected");
      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      // expect exception be thrown
      //log.info("vertex1={}", outGraph.getVertex("_" + 1));
      GraphTransaction.rollback(tx);
      assertEquals(1, Iterables.count(outGraph.getVertices()));
    }
  }

}
