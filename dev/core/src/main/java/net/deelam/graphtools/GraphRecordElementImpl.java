package net.deelam.graphtools;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@ToString
@Slf4j
public class GraphRecordElementImpl implements GraphRecordElement {
  private static final long serialVersionUID = 201509030419L;

  @Setter
  public static String LONG_ID_PROPKEY = "_longId";

  public GraphRecordElementImpl(String strId) {
    checkNotNull(strId);
    id = strId;
  }

  protected String id;

  public String getStringId() {
    return id;
  }

  @Override
  public Object getId() {
    return this.id;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (obj instanceof GraphRecordElement)
      return id.equals(((GraphRecordElement) obj).getId());
    return false;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Getter
  protected Map<String, Object> props = new HashMap<>();

  @Override
  public void setProperty(final String key, final Object value) {
    if(value==null)
      log.warn("Null value for key={} id={}", key, id);
    props.put(key, value);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T removeProperty(final String key) {
    return (T) props.remove(key);
  }

  public void clearProperties() {
    props.clear();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getProperty(final String key) {
    return (T) props.get(key);
  }

  @Override
  public Set<String> getPropertyKeys() {
    return props.keySet();
  }

  /// === UnsupportedOperations

  @Override
  public void remove() throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }
}
