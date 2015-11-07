package graphtools;

import static com.google.common.base.Preconditions.*;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.mutable.MutableInt;
import com.tinkerpop.blueprints.TransactionalGraph;

/**
 * This class does not create new objects.
 * 
 * Each begin() call must have a corresponding commit() or rollback() for all execution paths, including exceptions.
 * Best practice is to surround transactions with a try-catch block.
 * 
 * <pre>
 		int tx=GraphTransaction.begin(graph);
 		try{
			doYourGraphOperations(graph);
			doNestedTransaction(graph);  // contains calls to begin(), commit(), and maybe rollback()
			GraphTransaction.commit(tx);
		}catch(ExpectedException re){
			// expect exception be thrown
			GraphTransaction.rollback(tx);
			throw re;
		}
 </pre>
 * 
 * If doNestedTransaction() throws an uncaught exception (which means rollback() was not called),
 * GraphTransaction will handle it properly given the tx parameter.
 * 
 * Supports nested transactions.
 * 
 * When any of the nested transactions calls rollback(),
 * graph.rollback() actually occurs in the outermost transaction when commit() or rollback() is called.
 * 
 * @author deelam
 */
@Slf4j
@NoArgsConstructor
public class GraphTransaction {

	protected static final ThreadLocal<TransactionalGraph> graphHolder=new ThreadLocal<TransactionalGraph>();
	
	protected static final ThreadLocal<MutableInt> nestingCounter=new ThreadLocal<MutableInt>(){
		protected MutableInt initialValue(){
			return new MutableInt(0);
		}
	};

	protected static final ThreadLocal<AtomicBoolean> rollbackCalled=new ThreadLocal<AtomicBoolean>(){
		protected AtomicBoolean initialValue(){
			return new AtomicBoolean(false);
		};
	};

	/**
	 * @return depth of transaction.
	 */
	public static int begin(TransactionalGraph currGraph){
		checkNotNull(currGraph);
		TransactionalGraph g=graphHolder.get();
		//System.out.println("begin(TransactionalGraph "+currGraph);
		if(g == null){
			currGraph.commit(); // commit any previous transaction that may have been automatically started (e.g., due to read-only operations)
			graphHolder.set(currGraph);
			nestingCounter.get().increment();
			//System.out.println("Starting new outer-most transaction on "+currGraph);
			return nestingCounter.get().intValue();
		}else if(g == currGraph){ // intentionally not using equals()
			nestingCounter.get().increment();
			return nestingCounter.get().intValue();
		}else if(GraphUtils.isWrappedWithin(g, currGraph)){
			nestingCounter.get().increment();
			return nestingCounter.get().intValue();
		}else{
			log.error("This transaction only supports working with one graph ({}) at a time: {}\n"
				+ "   Or maybe you forgot to commit() or rollback() the previous transaction?", g, currGraph);
			throw new IllegalArgumentException("This transaction only supports working with one graph at a time: Expecting "
				+ g + " but got " + currGraph+".  The second graph is not equal to or nested in the first graph.  Also make sure there is a commit() for every begin().");
		}
	}

	/**
	 * Assumes commit() will not be called for the corresponding begin() associated with this transaction.
	 * @return if rollback actually occurred.  False means transaction is nested, and 
	 *     rollback is delayed until the top-level transaction is committed or rolledback.
	 */
	public static boolean rollback(int tx){
		updateNestingDepth(tx);

		if(!rollbackCalled.get().get()){
			rollbackCalled.get().set(true);
		}

		if(isOuterMostTransaction()){
			endTransaction();
			return true;
		}else{
			return false;
		}
	}

	private static void updateNestingDepth(int depth){
		if(depth < nestingCounter.get().intValue()){
			log.warn("Transaction depth mismatch: expected {} but got {}.  This can occur if you called rollback.  Readjusting depth.\n"
				+ "   Did you forget to catch an exception and call rollback?",
				nestingCounter.get().intValue(), depth);
			nestingCounter.get().setValue(depth - 1);
		} else {
			nestingCounter.get().decrement();
		}
	}

	/**
	 * 
	 * @return if top-level transaction was closed without rollback. False means (1) transaction was nested and is still open, or (2) rollback was called.
	 */
	public static boolean commit(int tx){
		updateNestingDepth(tx);

		if(isOuterMostTransaction()){
			return endTransaction();
		}else{
			log.trace("Depth={}, Not yet committing transaction on graph: {}", nestingCounter.get(), graphHolder.get());
			return false;
		}
	}

	public static boolean isInTransaction(){
		return graphHolder.get()!=null;
	}
	
	public static boolean isOuterMostTransaction(){
		return nestingCounter.get().intValue() == 0;
	}

	public static void checkTransactionsClosed(){
		checkState(!isInTransaction());
	}

	private static boolean endTransaction(){
		/// remove all ThreadLocal variables so they can be GC'd
		nestingCounter.remove();
		if(rollbackCalled.get().get()){
			rollbackCalled.remove();
			log.warn("Rolling back outer-most transaction on graph: {}", graphHolder.get());
			graphHolder.get().rollback();
			graphHolder.remove();
			return false;
		}else{
			rollbackCalled.remove();
			log.debug("Committing outer-most transaction on graph: {}", graphHolder.get());
			graphHolder.get().commit();
			graphHolder.remove();
			return true;
		}
	}

}

