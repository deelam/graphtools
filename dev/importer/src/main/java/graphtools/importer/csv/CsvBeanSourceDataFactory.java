/**
 * 
 */
package graphtools.importer.csv;

import graphtools.importer.SourceData;
import graphtools.importer.SourceDataFactory;

import java.io.File;
import java.io.FileNotFoundException;

import lombok.RequiredArgsConstructor;

/**
 * @author dlam
 *
 */
@RequiredArgsConstructor
public class CsvBeanSourceDataFactory<B> implements SourceDataFactory {

	private final CsvParser<B> parser;

	public SourceData<B> createFrom(File file) throws FileNotFoundException{
		return new CsvFileToBeanSourceData<B>(file, parser);	
	}
	
	public SourceData<B> createFrom(Readable readable){
		//Readable readable=new FileReader(file);
		return new CsvLineToBeanSourceData<B>(readable, parser);	
	}
}
