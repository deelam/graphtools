package net.deelam.graphtools;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.id.IdElement;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdVertex;

import net.deelam.graphtools.graphfactories.IdGraphFactoryTinker;

/**
 * 
 * @author deelam
 */
public class GraphHacks {

  public static void main(String[] args) {
    IdGraphFactoryTinker.register();
    IdGraph<?> graph=new GraphUri("tinker:").openIdGraph();
    IdVertex mdV = (IdVertex) GraphUtils.getMetaDataNode(graph);
    IdGraph<?> graph2 = getIdGraph(mdV);
    System.out.println(graph + " "+ graph2+ " "+(graph2==graph));
  }
  
  static IdGraph<?> getIdGraph(Element elem) {
    try {
      return (IdGraph<?>) idGraphField.get(elem);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      e.printStackTrace();
      return null;
    }
  }

  static Field idGraphField;

  static {
    try {
      idGraphField = getField(IdElement.class, "idGraph");
      makeAccessible(idGraphField);
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    }
  }

  private static Field getField(Class clazz, String fieldName) throws NoSuchFieldException {
    try {
      return clazz.getDeclaredField(fieldName);
    } catch (NoSuchFieldException e) {
      Class superClass = clazz.getSuperclass();
      if (superClass == null) {
        throw e;
      } else {
        return getField(superClass, fieldName);
      }
    }
  }

  private static void makeAccessible(Field field) {
    if (!Modifier.isPublic(field.getModifiers()) ||
        !Modifier.isPublic(field.getDeclaringClass().getModifiers())) {
      field.setAccessible(true);
    }
  }
}
