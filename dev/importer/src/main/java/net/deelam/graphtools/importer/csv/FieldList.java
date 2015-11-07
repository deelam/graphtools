package net.deelam.graphtools.importer.csv;

import java.util.ArrayList;

import org.apache.commons.lang3.tuple.Pair;
import org.supercsv.cellprocessor.*;
import org.supercsv.cellprocessor.ift.CellProcessor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class FieldList extends ArrayList<Pair<String, ? extends CellProcessor>> {
  private static final long serialVersionUID = 201502201325L;

  final Class<?> beanClass;

  // convenience method so that we can keep the arguments in case we want to
  // change from ignore to add()
  public void ignore(String ignoredFieldName, Object... ignoredArgs) {
    Pair<String, CellProcessor> pair = Pair.of(null, null);
    add(pair);
  }

  public void add(String fieldName) {
    add(fieldName, false);
  }

  public void add(String fieldName, CellProcessor processor) {
    add(Pair.of(fieldName, processor));
  }

  public void add(String fieldName, CellProcessor processor, boolean optional) {
    if (optional) {
      processor = new Optional(processor);
    }
    add(Pair.of(fieldName, processor));
  }

  public void add(String fieldName, boolean optional) {
    CellProcessor processor = null;
    try {
      Class<?> beanFieldClass = beanClass.getDeclaredField(fieldName).getType();
      if (String.class.equals(beanFieldClass)) {
        processor = new Optional(new Trim());
      } else if (Integer.class.equals(beanFieldClass)) {
        processor = new ParseInt();
      } else if (Long.class.equals(beanFieldClass)) {
        processor = new ParseLong();
      } else if (Double.class.equals(beanFieldClass)) {
        processor = new ParseDouble();
      } else if (Boolean.class.equals(beanFieldClass)) {
        processor = new ParseBool();
      } else {
        log.error("Unable to infer CellProcessor given: " + beanFieldClass);
      }

    } catch (NoSuchFieldException | SecurityException e) {
      log.error("Unable to infer CellProcessor given: " + fieldName, e);
      e.printStackTrace();
    }

    if (optional) {
      processor = new Optional(processor);
    }
    add(Pair.of(fieldName, processor));
  }

  public void add(String fieldName, Object beanField, CellProcessor processor) {
    // TODO: 9 (method not called): check that cellprocessor is appropriate
    // for bean field type
    add(Pair.of(fieldName, processor));
  }

  public String[] getCsvFields() {
    String[] csvFields = new String[size()];
    for (int i = 0; i < csvFields.length; ++i) {
      csvFields[i] = get(i).getLeft();
    }
    return csvFields;
  }

  public CellProcessor[] getCellProcessors() {
    CellProcessor[] procs = new CellProcessor[size()];
    for (int i = 0; i < procs.length; ++i) {
      procs[i] = get(i).getRight();
    }
    return procs;
  }
}
