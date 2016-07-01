package net.deelam.osgi;

import java.io.*;
import java.util.*;

import org.osgi.framework.*;
import org.osgi.framework.startlevel.BundleStartLevel;
import org.osgi.framework.startlevel.FrameworkStartLevel;

/**
 * Used to automatically find and install bundles in autoload.list files or autoload* directories.
 *  
 * If used for web apps, this class assumes the war file has been exploded so that files can be found.
 *  
 * To load bundles directly from the war file, use ServletContext.getResourcePaths(dir) -- 
 * see ProvisionActivator for an example.
 * 
 */
public class AutoLoader implements BundleActivator {

  private static final String AUTOLOAD_LIST_FILENAME = "autoload.list";
  // Event topic
  //	public static final String CF_BUNDLE_AUTOLOADER_DONE="cf/AutoLoader/done";

  @Override
  public void stop(BundleContext context) throws Exception {
    System.out.println("Deactivating: " + this);
  }

  private static final String[] AUTOLOAD_SEARCHPATH_PROPERTIES = {
      "AUTOLOADER.SEARCHPATH",
      "SERVLETCONTEXT.REALPATH"
  };

  private File getDirectoryToSearch(BundleContext context) {
    for (String path : AUTOLOAD_SEARCHPATH_PROPERTIES) {
      String scontextRealPath = System.getProperty(path);
      if (scontextRealPath != null) {
        System.out.println("Autoloader: Using " + path + "=" + scontextRealPath);
        return new File(scontextRealPath);
      }
    }

    File pluginsDir = getEquinoxBridgePluginDirectory(context);
    if (pluginsDir != null)
      return pluginsDir;

    String topDir = null;
    for (String prop : new String[] {"autoloadDirectory"}) {
      topDir = context.getProperty(prop);
      if (topDir != null)
        return new File(topDir);
    }
    return new File(".");
  }

  private File getEquinoxBridgePluginDirectory(BundleContext context) {
    File tmpDataFile = context.getDataFile(".");
    System.out.println("AutoLoader.getDataFile: " + tmpDataFile);
    String path = tmpDataFile.getAbsolutePath();
    int eclipseIndex = path.indexOf("eclipse/configuration");
    if (eclipseIndex > 0) {
      File pluginsDir = new File(path.substring(0, eclipseIndex) + "eclipse", "plugins");
      if (pluginsDir.exists())
        return pluginsDir;
    }
    return null;
  }

  @Override
  public void start(BundleContext context) throws Exception {
    File parentDir = getDirectoryToSearch(context);
    System.out.println("Autoloader: searching for " + AUTOLOAD_LIST_FILENAME
        + " file or autoload* subdirectories under "
        + parentDir.getAbsolutePath());
    List<File> matches = findAutoloadFiles(parentDir);

    if (matches.isEmpty()) {
      System.out.println("!! AutoLoader: 'autoload*' directories not found under " + matches);
    } else {
      List<Bundle> installedBundles = new ArrayList<Bundle>();
      for (File match : matches) {
        List<File> fileList;
        if (match.isDirectory()) {
          System.out.println("AutoLoader processsing directory: " + match);
          fileList = getFileTree(new LinkedList<File>(), match);
        } else {
          System.out.println("AutoLoader processsing file: " + match);
          fileList = parseAutoloadList(match);
        }
        installedBundles.addAll(installBundles(context, fileList));
      }
      startBundles(installedBundles);
      System.out.println("AutoLoader done.");
    }
    Dictionary<String, Object> properties = new Hashtable<String, Object>();
    properties.put("time", System.currentTimeMillis());
    //eventAdmin.postEvent(new Event(CF_BUNDLE_AUTOLOADER_DONE, properties));
  }

  private List<Bundle> installBundles(BundleContext context, List<File> fileList) {
    List<Bundle> installedBundles = new LinkedList<Bundle>();
    for (File f : fileList) {
      if (!f.getName().endsWith(".jar") || f.getName().endsWith("sources.jar"))
        continue;
      System.out.println(" Installing " + f.getAbsolutePath());
      try {
        installedBundles.add(context.installBundle("file:" + f.getAbsolutePath()));
      } catch (BundleException de) {
        System.err.println(de.getMessage());
      }
    }
    return installedBundles;
  }

  private void startBundles(List<Bundle> installedBundles) {
    int startLevel=1;
    /**
     * Remember to set the framework's startlevel
     * FrameworkStartLevel startLvl = framework.adapt(FrameworkStartLevel.class);
     * startLvl.setStartLevel(100);
     */
    for (Bundle bundle : installedBundles) {
      if (bundle.getLocation().contains("loadOnly")) {
        System.out.println(" Not starting: " + bundle);
      } else {
        BundleStartLevel bundleStartLevel = bundle.adapt(BundleStartLevel.class);
        bundleStartLevel.setStartLevel(++startLevel);
        if (bundle.getHeaders().get(Constants.FRAGMENT_HOST) == null) {
          try {
            System.out.println(" Starting " + bundle);
            bundle.start();
            ServiceReference<?>[] provides = bundle.getRegisteredServices();
            if (provides != null) {
              System.out.println("  provides service(s): \n\t    " +
                  Arrays.toString(provides).replace("}, {", "},\n\t     {"));
            }
          } catch (BundleException e) {
            e.printStackTrace();
          }
        } else {
          System.err.println(" Not starting fragment: " + bundle);
        }
      }
    }
  }

  private static List<File> findAutoloadFiles(File parentDir) {
    File[] candidates = parentDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        return file.isDirectory() || file.getName().equals(AUTOLOAD_LIST_FILENAME);
      }
    });

    if (candidates == null) {
      System.err.println("Could not find directory/file to autoload under " + parentDir);
      return new ArrayList<File>(0);
    }

    Arrays.sort(candidates);

    List<File> matching = new ArrayList<File>();
    for (int i = 0; i < candidates.length; ++i) {
      if (candidates[i].getName().startsWith("autoload")) {
        matching.add(candidates[i]);
        candidates[i] = null;
      }
    }

    for (int i = 0; i < candidates.length; ++i) {
      if (candidates[i] != null)
        matching.addAll(findAutoloadFiles(candidates[i]));
    }

    return matching;
  }

  private static List<File> getFileTree(List<File> files, File file) {
    if (!file.isDirectory()) {
      if (file.getName().equals(AUTOLOAD_LIST_FILENAME)) {
        files.addAll(parseAutoloadList(file));
      } else {
        files.add(file);
      }
    } else if (!file.getName().startsWith(".")) { // ignore ".svn" folders
      File[] current = file.listFiles();
      if (current != null) {
        Arrays.sort(current, new Comparator<File>() {
          // breath-first traversal: list files first
          public int compare(File o1, File o2) {
            if (o1.isDirectory())
              return o2.isDirectory() ? o1.compareTo(o2) : 1;
            else if (o2.isDirectory())
              return -1;
            return o1.compareTo(o2);
          }
        });

        for (File f : current) {
          getFileTree(files, f);
        }
      }
    }
    return files;
  }

  private static List<File> parseAutoloadList(File file) {
    List<File> list = new ArrayList<File>();
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        File f = new File(file.getParentFile(), line);
        if (f.isDirectory()) {
          getFileTree(list, f);
        } else if (f.exists()) {
          list.add(f);
        }
      }
      reader.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return list;
  }

  public static <T> T getServiceInstance(BundleContext context, Class<T> clazz) {
    ServiceReference<T> ref = context.getServiceReference(clazz);
    //System.out.println(context.getService(cmRef).getClass().getClassLoader());
    if (ref == null) {
      System.out.println("! ServiceReference not found for class=" + clazz);
      return null;
    }
    return context.getService(ref);
  }

  public static void main(String[] args) {
    List<File> dirs = findAutoloadFiles(new File("."));
    for (File dir : dirs) {
      List<File> files = getFileTree(new LinkedList<File>(), dir);
      for (File f : files)
        System.out.println(f);
    }

  }

}
