package net.deelam.graphtools.hadoop;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class HdfsUtils {

  @Getter
  private Configuration hadoopConf;

  public HdfsUtils(Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  synchronized Connection getHBaseConnection() throws IOException {
    { // To avoid IOException, https://github.com/brianfrankcooper/YCSB/issues/446
      // Doesn't stop error: hadoopConf.unset("hbase.dynamic.jars.dir"); // not needed for HDFS client
      //log.error("dynDir={} fileImpl={}", hadoopConf.get("hbase.dynamic.jars.dir"), hadoopConf.get("fs.file.impl"));
      //HadoopConfigurationHelper.print(hadoopConf, "conf4hbase.xml");
    }
    final Connection connection = ConnectionFactory.createConnection(hadoopConf);
    return connection;
  }

  public URI uploadFileToHdfsIfNeeded(URI sourceUri, String destDir) throws IOException, URISyntaxException {
    URI hdfsUri = sourceUri;
    // copy data to HDFS
    if (sourceUri.getScheme() == null || sourceUri.getScheme().equals("file")) {
      hdfsUri = uploadFile(new File(sourceUri), new Path(destDir), true).toUri();
    }
    if (!hdfsUri.getScheme().equals("hdfs"))
      log.error("Expecting 'hdfs' URI scheme but got {} for {}", hdfsUri.getScheme(), hdfsUri);
    return hdfsUri;
  }

  public String downloadFile(String src, String dst) throws IOException {
    Path srcPath = new Path(src);
    log.debug("Copying file: {} -> {}", srcPath, new Path(dst));
    try (FileSystem fs = FileSystem.get(hadoopConf)) {
      File dstFile = new File(dst);
      if(dst.endsWith("/")){
        if(!dstFile.exists()){
          log.info("Creating destination directory: {}", dstFile);
          dstFile.mkdirs();
        }
      }
      if(dstFile.exists() && dstFile.isDirectory())
        dst=new File(dst, srcPath.getName()).getAbsolutePath();
      fs.copyToLocalFile(false, srcPath, new Path(dst), true);
      log.info("Copied file to: {}", dstFile);
      return dst;
    }
  }
  
  public List<String> listDir(String path, boolean recursive) throws IOException {
    try (FileSystem fs = FileSystem.get(hadoopConf)) {
      List<String> files=new ArrayList<>();
      if (recursive) {
        // listFiles() only list files, not directories
        RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path(path), recursive);
        while (itr.hasNext()) {
          LocatedFileStatus s = itr.next();
          files.add(s.getPath().toString());
        }
      } else {
        FileStatus[] status = fs.listStatus(new Path(path));
        for (int i = 0; i < status.length; i++) {
          files.add(status[i].getPath().toString());
//          if (recursive && status[i].isDir()) {
//            files.addAll(listDir(status[i].getPath().toString(), recursive).get());
//          }
        }
      }
      return files;
    } catch (Throwable e){ // exceptions are not obvious on RPC client so print them here
      e.printStackTrace();
      throw e;
    }
  }

  public String uploadFile(File localFile, String destPath, boolean overwrite) throws IllegalArgumentException, IOException {
    try{
      uploadFile(localFile, new Path(destPath), overwrite);
      return destPath;
    } catch (Throwable e){ // exceptions are not obvious on RPC client so print them here
      e.printStackTrace();
      throw e;
    }
  }

  public static void main(String[] args) throws Exception {
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    HadoopTitanConfigs htConfigs = new HadoopTitanConfigs(null, null);
    HdfsUtils hdfs = htConfigs.getHdfsUtils();
    //hdfs.downloadFile("job_1440104812249_0584-csvRecordReaderErrfile-main-.txt", "errfile.txt"); // renames file
    //hdfs.downloadFile("adidis-demo", "hdfs-adidis-demo");  // renames dir if dest dir doesn't already exist; otherwise saves in existing dest dir
    
    //hdfs.uploadFile(new File("titan1.props"), ".", false); // copies file if doesn't exist
    //hdfs.uploadFile(new File("titan1.props"), ".", false); // throws Exception if exists
    //hdfs.uploadFile(new File("titan1.props"), ".", true);  // overwrites if exists
    
    //hdfs.uploadFile(new File("titan1.props"), "titan.props", false); // copies file as new name
    //hdfs.uploadFile(new File("titan1.props"), "titan.props", false); // throws Exception if exists
    //hdfs.uploadFile(new File("titan1.props"), "titan.props", true); // overwrites if exists
    
    //hdfs.uploadFile(new File("META-INF"), ".", false); // copies as subdir
    //hdfs.uploadFile(new File("META-INF"), ".", false); // throws Exception if exists
    //hdfs.uploadFile(new File("META-INF"), "M", false); // copies as new name
    //hdfs.uploadFile(new File("META-INF"), "M", false);  // copies as subdir if exists
    //hdfs.uploadFile(new File("META-INF"), "M", true);  // still copies as subdir if exists
    //hdfs.uploadFile(new File("META-INF"), "titan.props", true); // throws Exception if exists
    //hdfs.uploadFile(new File("META-INF"), "titan.propsDir", false); // copies as subdir
    
    //hdfs.uploadFile(new File("titan1.props"), "M", false); // copies file
    //hdfs.uploadFile(new File("titan1.props"), "M/mytitan.props", false); // copies file as new name
    
//    hdfs.uploadFile(new File("META-INF"), "M/N/O/P/Q", false); 
    
//    hdfs.delete("titan1.props");
//    hdfs.delete("titan.props");
//    hdfs.delete("titan.propsDir");
//    hdfs.delete("M");
//    hdfs.delete("META-INF");
    
/*    hdfs.listDir(".", false).stream().forEach(f->{
      System.out.println(f);
//      if(f.startsWith("job_144"))
//        try {
//          hdfs.delete(f);
//        } catch (Exception e) {
//          e.printStackTrace();
//        }
    });*/
    
    try (FileSystem fs = FileSystem.get(hdfs.hadoopConf)) {
      File srcFile=new File("target");
      Path src = new Path(srcFile.getAbsolutePath());
      System.out.println(srcFile.toURI());
      Path destDir=fs.makeQualified(new Path("."));
      System.out.println(src+" "+src.getParent());
      if(srcFile.isDirectory())
        src=src.getParent();
      Path dstFile = new Path(destDir, src.getName());
      System.out.println(dstFile);
      System.out.println(destDir);
      //hdfs.uploadFile(new File("META-INF"),".", false);
    }
  }
  
  public boolean exists(String path) throws IOException{
    try (FileSystem fs = FileSystem.get(hadoopConf)) {
      return fs.exists(new Path(path));
    }
  }
  
  public boolean isDirectory(String path) throws IOException{
    try (FileSystem fs = FileSystem.get(hadoopConf)) {
      return fs.isDirectory(new Path(path));
    }
  }
  
  public Path ensureDirExists(String path) throws IOException {
    Path p = new Path(path);
    try (FileSystem fs = FileSystem.get(hadoopConf)) {
      if(fs.exists(p))
        if(fs.isDirectory(p))
          return fs.makeQualified(p);
        else
          throw new FileAlreadyExistsException("Cannot create directory because file exists: "+p);
      else {
        if(fs.mkdirs(p))
          return fs.makeQualified(p);
        else
          throw new RuntimeException("fs.mkdirs() returned false: "+p);
      }
    }
  }

  public Path makeQualified(String path) throws IOException{
    return makeQualified(new Path(path));
  }
  
  public Path makeQualified(Path path) throws IOException{
    try (FileSystem fs = FileSystem.get(hadoopConf)) {
      return fs.makeQualified(path);
    }
  }

  public Path uploadFile(File srcFile, Path dest, boolean overwrite) throws IOException {
    if(!srcFile.exists())
      throw new FileNotFoundException("source file="+srcFile);
    try (FileSystem fs = FileSystem.get(hadoopConf)) {
      // WARNING: do not use file.toURI() as it will append a '/' if file is an existing directory,
      // which causes 'new Path(file.toURI()).getName()' to return empty string rather than the desired directory name
      Path src = new Path(srcFile.getAbsolutePath()); 
      dest=fs.makeQualified(dest);
      log.info("Received request to copy {} to {}", src, dest);

      if(srcFile.isDirectory()){
        if(fs.exists(dest)){
          if(fs.isDirectory(dest)){
            // even if overwrite=true, fs.copyFromLocalFile() still copies source directory as subdirectory
            log.info("Destination directory exists; copying source directory as subdirectory: {}", dest+"/"+srcFile.getName());
            return copyIntoDirectory(fs, src, dest, overwrite);  // tested
          } else { // dest is a file
            // even if overwrite=true, fs.copyFromLocalFile() throws FileAlreadyExistsException
            throw new FileAlreadyExistsException("Destination file exists: "+dest); // tested
          }
        } else {
          log.info("Copying source directory to new directory: {}", dest);
          return copy(fs, src, dest, overwrite); // tested
        }
      } else { // source is a file
        if(fs.exists(dest)){
          if(fs.isDirectory(dest)){
            log.info("Destination directory exists; copying source file within existing directory: {}", dest+"/"+srcFile.getName());
            return copyIntoDirectory(fs, src, dest, overwrite); // tested
          } else { // dest is a file
            if(overwrite){
              log.info("Destination file exists; deleting existing file and copying source file: {}", dest);
              return copy(fs, src, dest, overwrite); // tested
            }else{
              throw new FileAlreadyExistsException("Destination file exists: "+dest); // tested
            }
          }
        } else {
          log.info("Copying source file to new file: {}", dest);
          return copy(fs, src, dest, overwrite); // tested
        }
      }
    }
  }

  private Path copy(FileSystem fs, Path src, Path dest, boolean overwrite) throws IOException {
    fs.copyFromLocalFile(false, overwrite, src, dest);
    log.info("Copied to {}", dest);
    return dest;
  }

  private Path copyIntoDirectory(FileSystem fs, Path src, Path destDir, boolean overwrite)
      throws IOException {
    // must check in case destDir is an existing file and may be overridden 
    if(!fs.isDirectory(destDir))
      throw new IllegalArgumentException("Directory does not exist: "+destDir);
    Path dstFile = new Path(destDir, src.getName());
    if(!overwrite && fs.exists(dstFile))
      throw new FileAlreadyExistsException(dstFile.toString()); // otherwise fs.copyFromLocalFile() throws misleading IOException
    fs.copyFromLocalFile(false, overwrite, src, destDir);
    Path qualPath = fs.makeQualified(dstFile);
    log.info("Copied to {}", qualPath);
    return qualPath;
  }

  public Path mkdirs(String dir) throws IOException {
    try (FileSystem fs = FileSystem.get(hadoopConf)) {
      Path dirPath = new Path(dir);
      log.info("Making dir: {}", dir);
      fs.mkdirs(dirPath);
      return dirPath;
    }
  }
  
  public Iterable<Path> uploadFiles(String dest, File... files)
      throws IOException {
    List<Path> paths = Lists.newArrayList();
    Path destPath = mkdirs(dest);
    for (File f : files) {
      Path qualPath = uploadFile(f, destPath, true);
      paths.add(qualPath);
    }
    return paths;
  }

  public boolean delete(String hdfsTarget) throws IOException {
    Path target = new Path(hdfsTarget);
    try (FileSystem fs = FileSystem.get(hadoopConf)) {
      if (fs.exists(target) || hdfsTarget.contains("*") || hdfsTarget.contains("?")) {
        log.info("Deleting '" + target + "' in workingDir=" + fs.getWorkingDirectory());
        fs.delete(target, true); // delete file, true for recursive 
        return true;
      } else {
        log.warn("Not deleting; target does not exist: " + target + "  WorkingDir=" + fs.getWorkingDirectory());
      }
      return false;
    }
  }

  public String[] getTablenames() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    //Configuration hadoopConf=GetConfigurationsHelper.getHadoopConfig();
    try (Connection utils = getHBaseConnection()) {
      try (Admin admin = utils.getAdmin()) {
        TableName[] tns = admin.listTableNames();
        String[] names = new String[tns.length];
        for (int i = 0; i < tns.length; ++i) {
          names[i] = tns[i].toString();
        }
        return names;
      }
    }
  }

  public boolean hasTable(String tablename) throws IOException {
    checkNotNull(tablename);
    try (Connection utils = getHBaseConnection()) {
      try (Admin admin = utils.getAdmin()) {
        return admin.tableExists(TableName.valueOf(tablename));
      }
    }
    //Not working: HTableDescriptor[] tables=utils.listTables(tablename); tables.length > 0;
    //		for(String tn: getTablenames()){
    //			if(tablename.equals(tablename))
    //				return true;
    //		}
    //		return false;
  }

  public boolean deleteHBaseTable(String tablename) throws IOException {
    if (hasTable(tablename)) {
      try (Connection utils = getHBaseConnection()) {
        log.debug("Deleting table: " + tablename);
        TableName tn = TableName.valueOf(tablename);
        try (Admin admin = utils.getAdmin()) {
          try{
            admin.disableTable(tn);
          }catch(TableNotEnabledException e){
            log.info("TableNotEnabled: {}", tn);
          }
          admin.deleteTable(tn);
        }
        return true;
      }
    } else {
      log.info("Table does not exists: '{}'", tablename);
    }
    return false;
  }

}
