package net.deelam.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;

import lombok.AllArgsConstructor;
import lombok.extern.java.Log;
import net.deelam.common.util.JavaLoggingUtil;

// http://stackoverflow.com/questions/1732455/redirect-process-output-to-stdout
@AllArgsConstructor
@Log
public class StreamGobbler extends Thread {
  InputStream is;
  StreamLogLevel logLevel;

  public enum StreamLogLevel {
    OUT, ERR
  };

  public void run() {
    try (InputStreamReader isr = new InputStreamReader(is, Charset.forName("UTF-8"))) {
      BufferedReader br = new BufferedReader(isr);
      String line = null;
      while ((line = br.readLine()) != null) {
        switch (logLevel) {
          case ERR:
            log.severe(line);
            break;
          case OUT:
            log.info(line);
            break;
        }
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    JavaLoggingUtil.configureJUL("%1$tH:%1$tM:%1$tS [%4$s] %3$s: %5$s%6$s%n");
    // Execute command
    String command = "bash";
    Process process = Runtime.getRuntime().exec(command);

    new StreamGobbler(process.getErrorStream(), StreamGobbler.StreamLogLevel.OUT).start();
    new StreamGobbler(process.getInputStream(), StreamGobbler.StreamLogLevel.OUT).start();

    // Get output stream to write from it
    OutputStream out = process.getOutputStream();

    out.write("ls -l \n".getBytes());
    out.flush();
    out.write("echo 'hi'\n".getBytes());
    out.close();
  }
}
