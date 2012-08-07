/* 
 * Copyright 2012 Twitter, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.hdfsdu;

import com.google.common.io.Resources;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.AppLauncher;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.application.http.Registration;
import com.twitter.common.application.modules.HttpModule;
import com.twitter.common.application.modules.LogModule;
import com.twitter.common.application.modules.StatsModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.net.http.handlers.AssetHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.logging.Logger;

public class HdfsDu extends AbstractApplication {
  private static final Logger LOG = Logger.getLogger(HdfsDu.class.getName());

  private static final Database DB = new Database();
  protected static Connection conn = null;

  @CmdLine(name = "input_path", help = "Path to the input data set.")
  protected static final Arg<String> INPUT_PATH = Arg.create(null);

  @Inject
  private Lifecycle lifecycle;

  @CmdLine(name = "use_resources", help = "Use resources bundled in the jar")
  private static final Arg<Boolean> USE_RESOURCES = Arg.create(true);

  public void run() {
    LOG.info("Starting HdfsDu.");
    startDb();
    loadData();
    lifecycle.awaitShutdown();
  }

  private void startDb() {
    LOG.info("Creating table.");
    try {
      try {
        Class.forName("org.hsqldb.jdbc.JDBCDriver");
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("failed to load HSQLDB JDBC driver",
            e);
      }
      conn = DriverManager.getConnection("jdbc:hsqldb:mem:hdfsdu_test",
          "SA", "");
      Statement s = conn.createStatement();
      s.executeUpdate("drop table if exists size_by_path");
      s.executeUpdate("create table size_by_path (path varchar(4096), "
          + "size_in_bytes varchar(4096), file_count bigint, path_depth integer, leaf integer)");
    } catch (SQLException e) {
      throw new RuntimeException("omg", e);
    }
  }

  private void loadData() {
    Path inputPath;
    if (INPUT_PATH.get() != null) {
      inputPath = new Path(INPUT_PATH.get());
    } else {
      URL url = HdfsDu.class.getClassLoader().getResource(
          "com/twitter/hdfsdu/data/example.txt");
      if (url == null) {
        throw new RuntimeException("Failed getting example data.");
      }
      try {
        inputPath = new Path("file://" + url.toURI().getPath());
      } catch (URISyntaxException e) {
        throw new RuntimeException("Error loading example data.", e);
      }
    }

    try {
      Configuration conf = new Configuration();
      FSDataInputStream fsDataInputStream = inputPath.getFileSystem(conf)
          .open(inputPath);
      BufferedReader bufferedReader = new BufferedReader(
          new InputStreamReader(fsDataInputStream));

      String line;
      PreparedStatement s = conn
          .prepareStatement("insert into size_by_path "
              + "(path, size_in_bytes, file_count, path_depth, leaf) values (?, ?, ?, ?, ?)");
      while (true) {
        line = bufferedReader.readLine();
        if (line == null) {
          break;
        }
        String[] parts = line.split("\t");
        s.clearParameters();
        s.setString(1, parts[0]);
        s.setString(2, parts[1]);
        s.setLong(3, Long.parseLong(parts[2]));
        s.setInt(
            4,
            parts[0].split("/").length == 0 ? 0 : parts[0]
                .split("/").length - 1);
        s.setInt(5, Integer.parseInt(parts[3]));
        s.executeUpdate();

      }
      conn.commit();

      Statement statement = conn.createStatement();

      ResultSet results = statement
          .executeQuery("select count(*) from size_by_path");
      while (results.next()) {
        String cnt = results.getString(1);
        LOG.info("Count of rows in size_by_path table: " + cnt);
      }

      ResultSet resultSet = statement
          .executeQuery("select * from size_by_path limit 100");

      LOG.info("Sample lines read from DB:");
      while (resultSet.next()) {
        LOG.info(String.format(
            "path: %s  bytes: %s  file_count: %d  path_depth: %d",
            resultSet.getString("path"),
            resultSet.getString("size_in_bytes"),
            resultSet.getLong("file_count"),
            resultSet.getInt("path_depth")));
      }

    } catch (Exception e) {
      throw new RuntimeException("something bad happened", e);
    }
  }

  @Override
  public Iterable<? extends Module> getModules() {
    return Arrays.asList(
        new HttpModule(),
        new LogModule(),
        new StatsModule(),
        new HdfsDuModule());
  }

  public static void main(String... args) {
    AppLauncher.launch(HdfsDu.class, args);
  }

  private class HdfsDuModule extends AbstractModule {

    @Override
    protected void configure() {
      register("jquery/jquery.js", "application/javascript");
      register("d3/d3.js", "application/javascript");
      Registration.registerServlet(binder(), "/size_by_path",
          SizeByPathServlet.class, false);
      Registration.registerServlet(binder(), "/tree_size_by_path",
          TreeSizeByPathServlet.class, false);

      if (USE_RESOURCES.get()) {
        // JANK ALERT
        register("data/flare.json", "text/json");
        register("index.html", "text/html");
        register("index.js", "application/javascript");
        register("index.css", "text/css");
        register("filetree/filetree.css", "text/css");
        register("filetree/filetree.html", "text/html");
        register("filetree/filetree.js", "application/javascript");
        register("filetree/htmltools.js", "application/javascript");
        register("treemap/treemap.css", "text/css");
        register("treemap/treemap.html", "text/html");
        register("treemap/treemap.js", "application/javascript");
        register("jit/jit.treemap.js", "application/javascript");
        register("chroma/chroma.js", "application/javascript");
        register("range.png", "image/png");
      } else {
        Registration.registerServlet(binder(), "/data/flare.json",
            DataFlareJsonAssetHandler.class, false);
        Registration.registerServlet(binder(), "/index.html",
            IndexHtmlAssetHandler.class, false);
        Registration.registerServlet(binder(), "/index.js",
            IndexJsAssetHandler.class, false);
        Registration.registerServlet(binder(), "/index.css",
            IndexCssAssetHandler.class, false);
        Registration.registerServlet(binder(),
            "/filetree/filetree.css",
            FileTreeCssAssetHandler.class, false);
        Registration.registerServlet(binder(),
            "/filetree/filetree.html",
            FileTreeHtmlAssetHandler.class, false);
        Registration.registerServlet(binder(), "/filetree/filetree.js",
            FileTreeJsAssetHandler.class, false);
        Registration.registerServlet(binder(),
            "/filetree/htmltools.js",
            HtmlToolsJsAssetHandler.class, false);
        Registration.registerServlet(binder(), "/treemap/treemap.css",
            TreeMapCssAssetHandler.class, false);
        Registration.registerServlet(binder(), "/treemap/treemap.html",
            TreeMapHtmlAssetHandler.class, false);
        Registration.registerServlet(binder(), "/treemap/treemap.js",
            TreeMapJsAssetHandler.class, false);
        Registration.registerServlet(binder(), "/jit/jit.treemap.js",
            JitTreeMapJsAssetHandler.class, false);
        Registration.registerServlet(binder(), "/chroma/chroma.js",
            ChromaJsAssetHandler.class, false);
        Registration.registerServlet(binder(), "/range.png",
            ImageAssetHandler.class, false);
      }
    }

    private void register(String name, String type) {
      Registration.registerHttpAsset(binder(), "/" + name,
          HdfsDuModule.class, name, type, false);
    }
  }

  private static final String RESOURCES_DIR = "file:"
      + System.getProperty("user.dir")
      + "/service/src/main/resources/com/twitter/hdfsdu";

  private static class DataFlareJsonAssetHandler extends AssetHandler {
    public DataFlareJsonAssetHandler() throws MalformedURLException {
      super(new StaticAsset(Resources.newInputStreamSupplier(new URL(
          RESOURCES_DIR + "/data/flare.json")), "text/json", false));
    }
  }

  private static class FileTreeCssAssetHandler extends AssetHandler {
    public FileTreeCssAssetHandler() throws MalformedURLException {
      super(new StaticAsset(Resources.newInputStreamSupplier(new URL(
          RESOURCES_DIR + "/filetree/filetree.css")), "text/css",
          false));
    }
  }

  private static class FileTreeHtmlAssetHandler extends AssetHandler {
    public FileTreeHtmlAssetHandler() throws MalformedURLException {
      super(new StaticAsset(Resources.newInputStreamSupplier(new URL(
          RESOURCES_DIR + "/filetree/filetree.html")), "text/html",
          false));
    }
  }

  /**
   * super(new
   * StaticAsset(Resources.newInputStreamSupplier(Resources.getResource
   * (HdfsDu.class, "filetree/filetree.js")), "application/javascript",
   * false));
   */

  private static class FileTreeJsAssetHandler extends AssetHandler {
    public FileTreeJsAssetHandler() throws MalformedURLException {
      super(new StaticAsset(Resources.newInputStreamSupplier(new URL(
          RESOURCES_DIR + "/filetree/filetree.js")),
          "application/javascript", false));
    }
  }

  private static class HtmlToolsJsAssetHandler extends AssetHandler {
    public HtmlToolsJsAssetHandler() throws MalformedURLException {
      super(new StaticAsset(Resources.newInputStreamSupplier(new URL(
          RESOURCES_DIR + "/filetree/HtmlTools.js")),
          "application/javascript", false));
    }
  }

  private static class TreeMapCssAssetHandler extends AssetHandler {
    public TreeMapCssAssetHandler() throws MalformedURLException {
      super(
          new StaticAsset(Resources.newInputStreamSupplier(new URL(
              RESOURCES_DIR + "/treemap/treemap.css")),
              "text/css", false));
    }
  }

  private static class TreeMapHtmlAssetHandler extends AssetHandler {
    public TreeMapHtmlAssetHandler() throws MalformedURLException {
      super(new StaticAsset(Resources.newInputStreamSupplier(new URL(
          RESOURCES_DIR + "/treemap/treemap.html")), "text/html",
          false));
    }
  }

  private static class IndexHtmlAssetHandler extends AssetHandler {
    public IndexHtmlAssetHandler() throws MalformedURLException {
      super(new StaticAsset(Resources.newInputStreamSupplier(new URL(
          RESOURCES_DIR + "/index.html")), "text/html", false));
    }
  }

  private static class IndexJsAssetHandler extends AssetHandler {
    public IndexJsAssetHandler() throws MalformedURLException {
      super(new StaticAsset(Resources.newInputStreamSupplier(new URL(
          RESOURCES_DIR + "/index.js")), "application/javascript",
          false));
    }
  }

  private static class IndexCssAssetHandler extends AssetHandler {
    public IndexCssAssetHandler() throws MalformedURLException {
      super(new StaticAsset(Resources.newInputStreamSupplier(new URL(
          RESOURCES_DIR + "/index.css")), "text/css",
          false));
    }
  }

  private static class TreeMapJsAssetHandler extends AssetHandler {
    public TreeMapJsAssetHandler() throws MalformedURLException {
      super(new StaticAsset(Resources.newInputStreamSupplier(new URL(
          RESOURCES_DIR + "/treemap/treemap.js")),
          "application/javascript", false));
    }
  }

  private static class JitTreeMapJsAssetHandler extends AssetHandler {
    public JitTreeMapJsAssetHandler() throws MalformedURLException {
      super(new StaticAsset(Resources.newInputStreamSupplier(new URL(
          RESOURCES_DIR + "/jit/jit.treemap.js")),
          "application/javascript", false));
    }
  }

  private static class ChromaJsAssetHandler extends AssetHandler {
    public ChromaJsAssetHandler() throws MalformedURLException {
      super(new StaticAsset(Resources.newInputStreamSupplier(new URL(
          RESOURCES_DIR + "/chroma/chroma.js")),
          "application/javascript", false));
    }
  }

  private static class ImageAssetHandler extends AssetHandler {
    public ImageAssetHandler() throws MalformedURLException {
      super(new StaticAsset(Resources.newInputStreamSupplier(new URL(
          RESOURCES_DIR + "/range.png")), "image/png", false));
    }
  }
}
