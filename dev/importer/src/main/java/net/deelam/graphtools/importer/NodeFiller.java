package net.deelam.graphtools.importer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import com.tinkerpop.blueprints.Vertex;

@NoArgsConstructor
@AllArgsConstructor
public abstract class NodeFiller<C extends RecordContext<?>> {
  @Getter
  protected String type;

//  public NodeFiller<C> type(String nodeType) {
//    type = nodeType;
//    return this;
//  }

  abstract public String getId(C context);

  public void fill(Vertex v, C context) {}
}
