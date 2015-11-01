/**
 * 
 */
package graphtools.importer;

import graphtools.importer.csv.domain.TelephoneBean;

/**
 * Should not keep state across different sourceData.
 * Not thread safe -- create a new instance of each thread.
 * @author dlam
 */
public interface Encoder<B> {

	/**
	 * reinitialize for each new sourceData
	 * @param sourceData
	 */
	void reinit(SourceData<B> sourceData);
	
	/**
	 * called when no more data from sourceData
	 */
	void close(SourceData<B> sourceData);
	
	///

	RecordContext<B> createContext(B bean);

	int getEntityRelationCount(RecordContext<B> context);

	EntityRelation<? extends RecordContext<B>> getEntityRelation(int i);

}
