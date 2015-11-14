package net.deelam.graphtools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;


public final class PrettyPrintXml {

  public static void main(String[] args) throws Exception {
    if(args.length==0){
      System.out.println("Arguments: input.xml [output.xml]");
      System.exit(1);
    }
    
    String inFilename=args[0];
    String outFilename=null;
    if(args.length > 1)
      outFilename=args[1];
    
    prettyPrint(inFilename, outFilename);
  }

  public static void prettyPrint(String inFilename, String outFilename) throws Exception, IOException, FileNotFoundException {
    try (InputStream inFile = new FileInputStream(new File(inFilename))) {
      try (Writer out = (outFilename==null)? new StringWriter(): new BufferedWriter(new FileWriter(outFilename))) {
        prettyPrint(inFile, out);
        if (out instanceof StringWriter)
          System.out.println(out.toString());
      }
    }
  }

  public static final void prettyPrint(InputStream inputStream, Writer writer) throws Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setValidating(false);
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document xml = db.parse(inputStream);

    Transformer tf = TransformerFactory.newInstance().newTransformer();
    tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
    tf.setOutputProperty(OutputKeys.METHOD, "xml");
    tf.setOutputProperty(OutputKeys.INDENT, "yes");
    tf.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
    tf.transform(new DOMSource(xml), new StreamResult(writer));
  }

}
