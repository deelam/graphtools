package graphtools;
import java.util.Arrays;

import net.deelam.graphtools.PrettyPrintXml;

/**
 * Convenience class to run classes from the command line.
 * 
 * @author dleeam
 *
 */
public final class Run {
  public static void main(String[] args) throws Exception {
    if(args.length==0){
      System.out.println("Options: prettyPrintXml");
    }
    String cmd = args[0];
    
    args=Arrays.copyOfRange(args, 1, args.length);
    switch(cmd){
      case "prettyPrintXml":
        PrettyPrintXml.main(args);
        break;
      default:
        throw new UnsupportedOperationException("cmd="+cmd);
    }
  }
}
