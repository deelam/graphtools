package net.deelam;

import java.util.List;

import lombok.Getter;
import lombok.Setter;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.importer.EdgeFiller;
import net.deelam.graphtools.importer.Encoder;
import net.deelam.graphtools.importer.EntityRelation;
import net.deelam.graphtools.importer.NodeFiller;
import net.deelam.graphtools.importer.RecordContext;
import net.deelam.graphtools.importer.SourceData;

import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.Vertex;

public class ClassDepEncoder implements Encoder<ClassDepBean> {

  @Override
  public void reinit(SourceData<ClassDepBean> sourceData) {}

  @Override
  public void close(SourceData<ClassDepBean> sourceData) {}

  static class ClassDepRC implements RecordContext<ClassDepBean> {
    @Getter
    @Setter
    ClassDepBean bean;
    @Getter
    @Setter
    int instanceIndex;

    ClassDepRC reinit() {
      return this;
    }
  }

  private ClassDepRC rc = new ClassDepRC();

  @Override
  public RecordContext<ClassDepBean> createContext(ClassDepBean bean) {
    rc.setBean(bean);
    return rc.reinit(); // reuse instance
  }

  @Override
  public int getEntityRelationCount(RecordContext<ClassDepBean> context) {
    return graphFillers.size();
  }

  @Override
  public EntityRelation<ClassDepRC> getEntityRelation(int i, RecordContext<ClassDepBean> context) {
    return graphFillers.get(i);
  }

  @SuppressWarnings("unchecked")
  List<EntityRelation<ClassDepRC>> graphFillers = Lists.newArrayList(

      /// Jar -> Class
      new EntityRelation<ClassDepRC>(new SrcJarNodeFiller(), new SrcClassNodeFiller(),
          new ContainsEdgeFiller()),

      /// Jar -> Class
      new EntityRelation<ClassDepRC>(new DstJarNodeFiller(), new DstClassNodeFiller(),
          new ContainsEdgeFiller()),

          /// Class -> Class
      new EntityRelation<ClassDepRC>(new SrcClassNodeFiller(), new DstClassNodeFiller(),
          new DepOnEdgeFiller())


  );

  static final class SrcJarNodeFiller extends NodeFiller<ClassDepRC> {
    public SrcJarNodeFiller() {
      super("jar");
    }

    @Override
    public String getId(ClassDepRC context) {
      return "jar:" + context.bean.srcJar;
    }

    @Override
    public void fill(Vertex v, ClassDepRC context) {
      ClassDepBean b = context.bean;
    }
  }

  static final class DstJarNodeFiller extends NodeFiller<ClassDepRC> {
    public DstJarNodeFiller() {
      super("jar");
    }

    @Override
    public String getId(ClassDepRC context) {
      return "jar:" + context.bean.dstJar;
    }

    @Override
    public void fill(Vertex v, ClassDepRC context) {
      ClassDepBean b = context.bean;
    }
  }


  static final class SrcClassNodeFiller extends NodeFiller<ClassDepRC> {
    public SrcClassNodeFiller() {
      super("class");
    }

    @Override
    public String getId(ClassDepRC context) {
      return context.bean.srcClass;
    }

    @Override
    public void fill(Vertex v, ClassDepRC context) {
      ClassDepBean b = context.bean;
    }
  }
  static final class DstClassNodeFiller extends NodeFiller<ClassDepRC> {
    public DstClassNodeFiller() {
      super("class");
    }

    @Override
    public String getId(ClassDepRC context) {
      return context.bean.dstClass;
    }

    @Override
    public void fill(Vertex v, ClassDepRC context) {
      ClassDepBean b = context.bean;
    }
  }

  static final class ContainsEdgeFiller extends EdgeFiller<ClassDepRC> {
    public ContainsEdgeFiller() {
      super("contains");
    }

    @Override
    public String getId(GraphRecord outFv, GraphRecord inFv, ClassDepRC context) {
      return outFv.getId() + ":contains>" + inFv.getId();
    }
  }

  static final class DepOnEdgeFiller extends EdgeFiller<ClassDepRC> {
    public DepOnEdgeFiller() {
      super("dependsOn");
    }

    @Override
    public String getId(GraphRecord outFv, GraphRecord inFv, ClassDepRC context) {
      return outFv.getId() + ":depsOn>" + inFv.getId();
    }
  }

}
