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
	void setBean(B bean);
	void setInstanceIndex(int j);
	int getInstanceIndex();
}
