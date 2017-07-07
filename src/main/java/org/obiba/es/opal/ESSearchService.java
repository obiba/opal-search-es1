/*
 * Copyright (c) 2017 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.opal;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.rest.RestController;
import org.obiba.es.opal.support.ESQueryExecutor;
import org.obiba.es.opal.support.ESSearchQueryExecutor;
import org.obiba.opal.search.support.ValueTableIndexManager;
import org.obiba.opal.spi.search.*;
import org.obiba.opal.web.model.Search;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;

public class ESSearchService implements SearchService {

  private static final String ES_BRANCH = "2.x";

  private static final int TERMS_FACETS_SIZE_LIMIT = 200;

  private Properties properties;

  private boolean running;

  private Node esNode;

  private Client client;

  private SearchSettings settings;

  private VariableSummaryHandler variableSummaryHandler;

  private ThreadFactory threadFactory;

  private VariablesIndexManager variablesIndexManager;

  private ValuesIndexManager valuesIndexManager;

  //
  // Service management
  //

  @Override
  public String getName() {
    return "opal-search-es";
  }

  @Override
  public Properties getProperties() {
    return properties;
  }

  @Override
  public void configure(Properties properties) {
    this.properties = properties;
  }

  @Override
  public void configure(SearchSettings settings, VariableSummaryHandler variableSummaryHandler, ThreadFactory threadFactory) {
    this.settings = settings;
    this.variableSummaryHandler = variableSummaryHandler;
    this.threadFactory = threadFactory;
  }

  @Override
  public SearchSettings getConfig() {
    return settings;
  }

  @Override
  public boolean isEnabled() {
    return settings != null && settings.isEnabled();
  }

  @Override
  public boolean isRunning() {
    return running && esNode != null;
  }

  @Override
  public void start() {
    // do init stuff
    if (settings != null) {
      File pluginWorkDir = new File(getWorkFolder(), ES_BRANCH);
      Settings.Builder builder = Settings.settingsBuilder() //
          .put("path.home", getInstallFolder().getAbsolutePath()) //
          .put("path.data", new File(pluginWorkDir, "data").getAbsolutePath()) //
          .put("path.work", new File(pluginWorkDir, "work").getAbsolutePath());

      File defaultSettings = new File(getInstallFolder(), "elasticsearch.yml");
      if (defaultSettings.exists())
        builder.loadFromPath(defaultSettings.toPath());

      builder.loadFromSource(settings.getEsSettings());

      esNode = NodeBuilder.nodeBuilder() //
          .client(!isDataNode()) //
          .settings(builder) //
          .clusterName(getClusterName()) //
          .node();
      client = esNode.client();
      running = true;
    }
  }

  @Override
  public void stop() {
    running = false;
    if (esNode != null) esNode.close();
    esNode = null;
    client = null;
    valuesIndexManager = null;
    variablesIndexManager = null;
  }

  //
  // Index methods
  //

  @Override
  public VariablesIndexManager getVariablesIndexManager() {
    if (variablesIndexManager == null)
      variablesIndexManager = new ESVariablesIndexManager(this);
    return variablesIndexManager;
  }

  @Override
  public ValuesIndexManager getValuesIndexManager() {
    if (valuesIndexManager == null)
      valuesIndexManager = new ESValuesIndexManager(this, variableSummaryHandler, threadFactory);
    return valuesIndexManager;
  }

  //
  // Search methods
  //

  @Override
  public JSONObject executeQuery(JSONObject jsonQuery, String searchPath) throws JSONException {
    ESQueryExecutor executor = new ESQueryExecutor(this).setSearchPath(searchPath);
    return executor.execute(jsonQuery);
  }

  @Override
  public Search.QueryResultDto executeQuery(String datasource, String table, Search.QueryTermDto queryDto) throws JSONException {
    return createQueryExecutor(datasource, table).execute(queryDto);
  }

  @Override
  public Search.QueryResultDto executeQuery(String datasource, String table, Search.QueryTermsDto queryDto) throws JSONException {
    return createQueryExecutor(datasource, table).execute(queryDto);
  }

  //
  // ES methods
  //

  public Client getClient() {
    return client;
  }

  public RestController newRestController() {
    return esNode.injector().getInstance(RestController.class);
  }

  //
  // Private methods
  //


  private SearchQueryExecutor createQueryExecutor(String datasource, String table) {
    ValueTableIndexManager valueTableIndexManager = new ValueTableIndexManager(getValuesIndexManager(), datasource, table);
    return new ESSearchQueryExecutor(this, valueTableIndexManager, getTermsFacetSizeLimit());
  }

  private int getTermsFacetSizeLimit() {
    try {
      return Integer.parseInt(properties.getProperty("termsFacetSizeLimit", "" + TERMS_FACETS_SIZE_LIMIT));
    } catch (NumberFormatException e ) {
      return TERMS_FACETS_SIZE_LIMIT;
    }
  }

  private File getDataFolder() {
    return getServiceFolder(DATA_DIR_PROPERTY);
  }

  private File getWorkFolder() {
    return getServiceFolder(WORK_DIR_PROPERTY);
  }

  private File getInstallFolder() {
    return getServiceFolder(INSTALL_DIR_PROPERTY);
  }

  private File getServiceFolder(String dirProperty) {
    String defaultDir = new File(".").getAbsolutePath();
    String dataDirPath = properties.getProperty(dirProperty, defaultDir);
    File dataDir = new File(dataDirPath);
    if (!dataDir.exists()) dataDir.mkdirs();
    return dataDir;
  }

  private String getClusterName() {
    return settings == null ? "opal" : settings.getClusterName();
  }

  private boolean isDataNode() {
    return settings == null ? true : settings.isDataNode();
  }
}
