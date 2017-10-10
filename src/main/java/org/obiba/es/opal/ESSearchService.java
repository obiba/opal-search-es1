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

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.obiba.es.opal.support.ESQueryExecutor;
import org.obiba.es.opal.support.ESSearchQueryExecutor;
import org.obiba.es.opal.support.QueryResultConverter;
import org.obiba.opal.spi.search.*;
import org.obiba.opal.spi.search.support.ItemResultDtoStrategy;
import org.obiba.opal.spi.search.support.ValueTableIndexManager;
import org.obiba.opal.web.model.Search;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class ESSearchService implements SearchService {

  private static final String ES_BRANCH = "2.4.x";

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
    return running && client != null;
  }

  @Override
  public void start() {
    // do init stuff
    if (settings != null) {
      File pluginWorkDir = new File(getWorkFolder(), properties.getProperty("es.version", ES_BRANCH));
      Settings.Builder builder = Settings.settingsBuilder() //
          .put("path.home", getInstallFolder().getAbsolutePath()) //
          .put("path.data", new File(pluginWorkDir, "data").getAbsolutePath()) //
          .put("path.work", new File(pluginWorkDir, "work").getAbsolutePath());

      File defaultSettings = new File(getInstallFolder(), "elasticsearch.yml");
      if (defaultSettings.exists())
        builder.loadFromPath(defaultSettings.toPath());

      builder.loadFromSource(settings.getEsSettings())
          .put("cluster.name", getClusterName());

      if (isTransportClient())
        createTransportClient(builder);
      else
        createNodeClient(builder);

      running = true;
    }
  }

  @Override
  public void stop() {
    running = false;
    if (esNode != null) esNode.close();
    if (client != null) client.close();
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
  public void executeIdentifiersQuery(QuerySettings querySettings, String searchPath, HitsQueryCallback<String> callback) throws SearchException {
    JSONObject jsonResponse = executeQuery(querySettings, searchPath);
    if (jsonResponse.isNull("error")) {
      try {
        JSONObject jsonHits = jsonResponse.getJSONObject("hits");
        callback.onTotal(jsonHits.getInt("total"));
        JSONArray hits = jsonHits.getJSONArray("hits");
        for (int i = 0; i < hits.length(); i++) {
          JSONObject jsonHit = hits.getJSONObject(i);
          if (jsonHit.has("_source")) {
            callback.onIdentifier(jsonHit.getJSONObject("_source").getString("identifier"));
          }
        }
      } catch (JSONException e) {
        throw new SearchException(e.getMessage(), e);
      }
    } else callback.onTotal(0);
  }

  @Override
  public Search.QueryResultDto executeQuery(QuerySettings querySettings, String searchPath, ItemResultDtoStrategy strategy) throws SearchException {
    JSONObject jsonResponse = executeQuery(querySettings, searchPath);
    QueryResultConverter converter = new QueryResultConverter();
    if (strategy != null) converter.setStrategy(strategy);
    try {
      return converter.convert(jsonResponse);
    } catch (JSONException e) {
      throw new SearchException(e.getMessage(), e);
    }
  }

  public Search.EntitiesResultDto.Builder executeEntitiesQuery(QuerySettings querySettings, String searchPath, String entityType, String query) throws SearchException {
    JSONObject jsonResponse = executeQuery(querySettings, searchPath);
    Search.EntitiesResultDto.Builder builder = Search.EntitiesResultDto.newBuilder();
    builder.setEntityType(entityType);
    try {
      if (jsonResponse.has("hits")) {
        JSONObject jsonHits = jsonResponse.getJSONObject("hits");
        builder.setTotalHits(jsonHits.getInt("total"));
        builder.setQuery(query);

        JSONArray hits = jsonHits.getJSONArray("hits");
        if (hits.length() > 0) {
          for (int i = 0; i < hits.length(); i++) {
            Search.ItemResultDto.Builder dtoItemResultBuilder = Search.ItemResultDto.newBuilder();
            JSONObject jsonHit = hits.getJSONObject(i);
            dtoItemResultBuilder.setIdentifier(jsonHit.getString("_id"));
            builder.addHits(dtoItemResultBuilder);
          }
        }
      } else {
        builder.setTotalHits(0);
        builder.setQuery(query);
      }
      return builder;
    } catch (JSONException e) {
      throw new SearchException(e.getMessage(), e);
    }
  }

  @Override
  public Search.QueryResultDto executeQuery(String datasource, String table, Search.QueryTermDto queryDto) throws SearchException {
    return createQueryExecutor(datasource, table).execute(queryDto);
  }

  @Override
  public Search.QueryResultDto executeQuery(String datasource, String table, Search.QueryTermsDto queryDto) throws SearchException {
    return createQueryExecutor(datasource, table).execute(queryDto);
  }

  //
  // ES methods
  //

  public Client getClient() {
    return client;
  }

  //
  // Private methods
  //

  private JSONObject executeQuery(QuerySettings querySettings, String searchPath) throws SearchException {
    ESQueryExecutor executor = new ESQueryExecutor(this).setSearchPath(searchPath);
    try {
      return executor.execute(querySettings);
    } catch (JSONException e) {
      throw new SearchException(e.getMessage(), e);
    }
  }

  private SearchQueryExecutor createQueryExecutor(String datasource, String table) {
    ValueTableIndexManager valueTableIndexManager = new ValueTableIndexManager(getValuesIndexManager(), datasource, table);
    return new ESSearchQueryExecutor(this, valueTableIndexManager, getTermsFacetSizeLimit());
  }

  private int getTermsFacetSizeLimit() {
    try {
      return Integer.parseInt(properties.getProperty("termsFacetSizeLimit", "" + TERMS_FACETS_SIZE_LIMIT));
    } catch (NumberFormatException e) {
      return TERMS_FACETS_SIZE_LIMIT;
    }
  }

  private void createTransportClient(Settings.Builder builder) {
    builder.put("client.transport.sniff", isTransportSniff());
    final TransportClient transportClient = TransportClient.builder().settings(builder.build()).build();
    getTransportAddresses().forEach(ta -> {
      int port = 9300;
      String host = ta;
      int sepIdx = ta.lastIndexOf(':');

      if (sepIdx > 0) {
        port = Integer.parseInt(ta.substring(sepIdx + 1, ta.length()));
        host = ta.substring(0, sepIdx);
      }

      try {
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
      } catch (UnknownHostException e) {
        Throwables.propagate(e);
      }
    });
    client = transportClient;
  }

  private void createNodeClient(Settings.Builder builder) {
    esNode = NodeBuilder.nodeBuilder() //
        .client(!isDataNode()) //
        .settings(builder) //
        .clusterName(getClusterName()) //
        .node();
    client = esNode.client();
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

  private boolean isTransportClient() {
    return Boolean.parseBoolean(properties.getProperty("transportClient", "false"));
  }

  private boolean isTransportSniff() {
    return Boolean.parseBoolean(properties.getProperty("transportSniff", "false"));
  }

  private List<String> getTransportAddresses() {
    String addStr = properties.getProperty("transportAddresses", "").trim();
    return addStr.isEmpty() ? Lists.newArrayList() :
        Stream.of(addStr.split(",")).map(String::trim).collect(toList());
  }

  private boolean isDataNode() {
    return settings == null || settings.isDataNode();
  }
}
