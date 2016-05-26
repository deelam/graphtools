/**
 * 
 */
package net.deelam.graphtools;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;
import com.tinkerpop.blueprints.util.wrappers.readonly.ReadOnlyTokens;

/**
 * @author dd
 *
 */
public class ReadOnlyIdGraph extends IdGraph<KeyIndexableGraph> {

  /**
   */
  public ReadOnlyIdGraph(KeyIndexableGraph baseGraph) {
    super(baseGraph);
  }
  
  /**
   * @throws UnsupportedOperationException
   */
  public void removeVertex(final Vertex vertex) throws UnsupportedOperationException {
      throw new UnsupportedOperationException(ReadOnlyTokens.MUTATE_ERROR_MESSAGE);
  }

  /**
   * @throws UnsupportedOperationException
   */
  public Edge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) throws UnsupportedOperationException {
      throw new UnsupportedOperationException(ReadOnlyTokens.MUTATE_ERROR_MESSAGE);
  }

  /**
   * @throws UnsupportedOperationException
   */
  public void removeEdge(final Edge edge) throws UnsupportedOperationException {
      throw new UnsupportedOperationException(ReadOnlyTokens.MUTATE_ERROR_MESSAGE);
  }

  /**
   * @throws UnsupportedOperationException
   */
  public Vertex addVertex(final Object id) throws UnsupportedOperationException {
      throw new UnsupportedOperationException(ReadOnlyTokens.MUTATE_ERROR_MESSAGE);
  }

}
