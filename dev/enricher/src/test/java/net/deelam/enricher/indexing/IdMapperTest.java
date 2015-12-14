package net.deelam.enricher.indexing;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class IdMapperTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void basicTest() throws Exception {
    String file = "target/graphUri.map";
    new File(file).delete();
    
    IdMapper nm = new IdMapper(file);
    String longStrId = "tinker:some/long/path/you/dont/want/to/store";

    String shortStrId = nm.getShortIdIfExists(longStrId);
    assertEquals(0, nm.getMap().size());
    if (shortStrId == null)
      shortStrId = nm.shortId(longStrId);

    assertEquals(1, nm.getMap().size());
    
    String lStrId = nm.longId(shortStrId);
    assertEquals(longStrId, lStrId);

    String shortStrId2 = nm.shortId("123" + longStrId);
    assertNotSame(shortStrId, shortStrId2);

    assertEquals(2, nm.getMap().size());
    
    String lStrId2 = nm.longId(shortStrId2);
    assertEquals("123" + longStrId, lStrId2);

    // test override shortName with new longName
    String longNeoId = "neo4j:new/graph/location";
    nm.replaceLongId(shortStrId, longNeoId);
    String lStrIdNeo = nm.longId(shortStrId);
    assertEquals(longNeoId, lStrIdNeo);

    { // check other entry hasn't changed
      lStrId2 = nm.longId(shortStrId2);
      assertEquals("123" + longStrId, lStrId2);
    }
    { // test override longName with new shortName
      nm.replaceShortId("myId", "123" + longStrId);
      String myId = nm.shortId("123" + longStrId);
      assertEquals("myId", myId);
    }

    assertEquals(2, nm.getMap().size());
    try {
      // test override unknown shortName
      nm.replaceLongId("myId_SOMETHING", "123" + longStrId);
      fail("Should not be able to replace id that doesn't exist!");
    } catch (RuntimeException e) {
    }
    try {
      // test override unknown shortName
      nm.replaceShortId("myId", "SOMETHING_123" + longStrId);
      fail("Should not be able to replace id that doesn't exist!");
    } catch (RuntimeException e) {
    }

    {
      String myId = nm.shortId("123" + longStrId);
      assertEquals("myId", myId);
    }
    
    nm.close();
    
    /// reload from file
    
    nm=new IdMapper(file);
    assertEquals(2, nm.getMap().size());
    {
      String myId = nm.shortId("123" + longStrId);
      assertEquals("myId", myId);
    }
    
    {
      nm.replaceShortId("_3", "123"+longStrId);
      String newId = nm.shortId("my new long id");
      assertEquals("_4", newId);
    }
    
  }
}
