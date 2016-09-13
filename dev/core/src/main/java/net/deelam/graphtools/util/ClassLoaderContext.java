package net.deelam.graphtools.util;

import lombok.extern.java.Log;

/**
 * 
 * @author dnlam, Created:Sep 23, 2015
 */
@Log
public class ClassLoaderContext implements AutoCloseable {
	
	public ClassLoaderContext(Class<?> clazz){
		setContextClassLoader(clazz.getClassLoader());
	}

	public ClassLoaderContext(ClassLoader cl){
		setContextClassLoader(cl);
	}

	private ClassLoader tccl;

	/**
	 * Since all REST API calls will have the jetty bundle as the classloader, 
	 * it won't have access to Hadoop and other application classes.
	 * @param clazz 
	 * @param cl 
	 */
	private ClassLoader setContextClassLoader(ClassLoader cl){
		tccl=Thread.currentThread().getContextClassLoader();
		// http://stackoverflow.com/questions/18272268/hbase-default-xml-file-seems-to-be-for-and-old-version-of-hbase-null-this-ver
		Thread.currentThread().setContextClassLoader(cl);
		log.fine(()-> "Set ContextClassLoader="+Thread.currentThread().getContextClassLoader());
		return tccl;
	}

	private void restoreContextClassLoader(ClassLoader tccl){
		Thread.currentThread().setContextClassLoader(tccl);
		log.fine(()->"Restored ContextClassLoader to "+Thread.currentThread().getContextClassLoader());
	}

	@Override
	public void close(){
		restoreContextClassLoader(tccl);
	}
}