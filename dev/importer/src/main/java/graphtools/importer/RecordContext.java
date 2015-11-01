/**
 * 
 */
package graphtools.importer;

/**
 * @author deelam
 *
 */
public interface RecordContext<B> {
	B getBean();
	void setInstanceIndex(int j);
	int getInstanceIndex();
}
