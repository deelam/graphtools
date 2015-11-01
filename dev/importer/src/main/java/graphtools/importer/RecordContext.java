/**
 * 
 */
package graphtools.importer;

/**
 * @author dlam
 *
 */
public interface RecordContext<B> {
	B getBean();
	void setInstanceIndex(int j);
	int getInstanceIndex();
}
