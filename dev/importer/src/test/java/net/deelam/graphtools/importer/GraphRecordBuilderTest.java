package net.deelam.graphtools.importer;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collection;

import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphRecordImpl;
import net.deelam.graphtools.importer.csv.CsvLineToBeanSourceData;
import net.deelam.graphtools.importer.csv.CsvParser;
import net.deelam.graphtools.importer.domain.CompanyContactBean;
import net.deelam.graphtools.importer.domain.CompanyContactsCsvParser;
import net.deelam.graphtools.importer.domain.CompanyContactsEncoder;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author deelam, Created:Nov 10, 2015
 */
public class GraphRecordBuilderTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  public <B> CsvLineToBeanSourceData<B> initSrcData(String parserClassname) {
    CsvParser<B> parser = instantiate(parserClassname);
    return new CsvLineToBeanSourceData<>(parser);
  }
  public <B> GraphRecordBuilder<B> initGRBuilder(String encoderClassname) {
    Encoder<B> encoder = instantiate(encoderClassname);
    GraphRecord.Factory grFactory=new GraphRecordImpl.Factory();
    return new GraphRecordBuilder<>(encoder, grFactory);
  }
  
  static <T> T instantiate(String className) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends T> myclass = (Class<? extends T>) Class.forName(className);
      T csvParsingConstants = myclass.newInstance();
      return csvParsingConstants;
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      e.printStackTrace();
      return null;
    }
  }
  
  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void test() throws IOException {
    CsvLineToBeanSourceData<CompanyContactBean> srcData = initSrcData(CompanyContactsCsvParser.class.getName());
    GraphRecordBuilder<CompanyContactBean> grBuilder = initGRBuilder(CompanyContactsEncoder.class.getName());
    String rowStr="James,Butt,\"Benton, John B Jr\",6649 N Blue Gum St,New Orleans,Orleans,LA,70116,504-621-8927,504-845-1427,jbutt@gmail.com,http://www.bentonjohnbjr.com";
    CompanyContactBean bean = srcData.parse(rowStr);
    if (bean == null) {
      fail("should not be null");
    }
    Collection<GraphRecord> gr = grBuilder.build(bean);
    System.out.println(gr);
    assertEquals(6, gr.size());
  }

}
