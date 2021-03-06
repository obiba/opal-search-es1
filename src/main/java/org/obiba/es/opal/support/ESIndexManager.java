/*
 * Copyright (c) 2018 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.obiba.es.opal.support;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.obiba.es.opal.ESSearchService;
import org.obiba.magma.Timestamps;
import org.obiba.magma.Value;
import org.obiba.magma.ValueTable;
import org.obiba.magma.support.MagmaEngineTableResolver;
import org.obiba.magma.support.Timestampeds;
import org.obiba.magma.type.DateTimeType;
import org.obiba.opal.spi.search.IndexManager;
import org.obiba.opal.spi.search.IndexSynchronization;
import org.obiba.opal.spi.search.ValueTableIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public abstract class ESIndexManager implements IndexManager {

  private static final Logger log = LoggerFactory.getLogger(ESIndexManager.class);

  protected static final int ES_BATCH_SIZE = 100;

  protected final ESSearchService esSearchService;

  private final Map<String, ValueTableIndex> indices = Maps.newHashMap();

  protected ESIndexManager(ESSearchService esSearchService) {
    this.esSearchService = esSearchService;
  }

  @Override
  public ValueTableIndex getIndex(@NotNull ValueTable vt) {
    String tableFullName = vt.getTableReference();
    ValueTableIndex index = indices.computeIfAbsent(tableFullName, k -> createIndex(vt));
    return index;
  }

  @Override
  public boolean hasIndex(@NotNull ValueTable valueTable) {
    ClusterStateResponse resp = esSearchService.getClient().admin().cluster().prepareState().execute().actionGet();
    ValueTableIndex valueTableIndex = getIndex(valueTable);
    IndexMetaData indexMetaData = resp.getState().metaData().index(valueTableIndex.getIndexName());
    if (indexMetaData == null) return false;
    ImmutableOpenMap<String, MappingMetaData> mappings = indexMetaData.getMappings();
    return mappings.containsKey(valueTableIndex.getIndexType());
  }

  protected abstract ValueTableIndex createIndex(@NotNull ValueTable vt);

  @Override
  public boolean isEnabled() {
    return esSearchService.getConfig().isEnabled();
  }

  @Override
  public boolean isReady() {
    return esSearchService.isEnabled() && esSearchService.getConfig().isEnabled();
  }

  @Override
  public void drop() {
    esSearchService.getClient().admin().indices().prepareDelete(getName()).execute();
    indices.clear();
  }

  @Override
  public boolean isIndexUpToDate(@NotNull ValueTable valueTable) {
    return getIndex(valueTable).isUpToDate();
  }

  protected String esIndexName() {
    return esSearchService.getConfig().getIndexName();
  }

  protected Settings getIndexSettings() {
    return Settings.settingsBuilder()
        .put(esSearchService.getClient().settings().getByPrefix("index."))
        .put("number_of_shards", esSearchService.getConfig().getShards())
        .put("number_of_replicas", esSearchService.getConfig().getReplicas()).build();
  }

  protected abstract class ESIndexer implements IndexSynchronization {

    @NotNull
    protected final ValueTable valueTable;

    @NotNull
    private final ESValueTableIndex index;

    private final int total;

    protected int done = 0;

    protected boolean stop = false;

    protected ESIndexer(@NotNull ValueTable table, @NotNull ESValueTableIndex index) {
      valueTable = table;
      this.index = index;
      total = valueTable.getVariableEntityCount();
    }

    @Override
    public IndexManager getIndexManager() {
      return ESIndexManager.this;
    }

    @Override
    public void run() {
      log.debug("Updating ValueTable index {}", index.getValueTableReference());
      index.delete();
      index.createIndex();
      index();
    }

    protected BulkRequestBuilder sendAndCheck(BulkRequestBuilder bulkRequest) {
      if (bulkRequest.numberOfActions() > 0) {
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
          // process failures by iterating through each bulk response item
          throw new RuntimeException(bulkResponse.buildFailureMessage());
        }
        return esSearchService.getClient().prepareBulk();
      }
      return bulkRequest;
    }

    protected abstract void index();

    @Override
    public ValueTableIndex getValueTableIndex() {
      return index;
    }

    @NotNull
    @Override
    public ValueTable getValueTable() {
      return valueTable;
    }

    @Override
    public boolean hasStarted() {
      return done > 0;
    }

    @Override
    public boolean isComplete() {
      return total > 0 && done >= total;
    }

    @Override
    public float getProgress() {
      return done / (float) total;
    }

    @Override
    public void stop() {
      stop = true;
    }
  }

  protected abstract class ESValueTableIndex implements ValueTableIndex {

    static final int MAX_SIZE = 10000;

    @NotNull
    protected final String name;

    @NotNull
    private final String valueTableReference;

    private ESMapping esMapping;

    /**
     * @param vt
     */
    protected ESValueTableIndex(@NotNull ValueTable vt) {
      name = indexName(vt);
      valueTableReference = vt.getTableReference();
    }

    @NotNull
    @Override
    public String getIndexName() {
      return getName();
    }

    @NotNull
    public String getValueTableReference() {
      return valueTableReference;
    }

    public void updateTimestamps() {
      try {
        // reset metadata cache
        esMapping = null;
        ESMapping mapping = readMapping();
        //noinspection ConstantConditions
        mapping.meta().setString(name, DateTimeType.get().valueOf(new Date()).toString());
        esSearchService.getClient().admin().indices().preparePutMapping(getIndexName()).setType(getIndexType())
            .setSource(mapping.toXContent()).execute().actionGet();
        // reset metadata cache
        esMapping = null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void delete() {
      // reset metadata cache
      esMapping = null;
      if (!esSearchService.isEnabled() || !esSearchService.isRunning()) return;
      BulkRequestBuilder bulkRequest = esSearchService.getClient().prepareBulk();
      // TODO remove table's items

      try {
        long total = MAX_SIZE;
        int from = 0;
        while (from < total) {
          QueryBuilder query = QueryBuilders.termQuery("reference", getValueTableReference());
          SearchRequestBuilder search = esSearchService.getClient().prepareSearch() //
              .setIndices(getIndexName()) //
              .setTypes(getIndexType()) //
              .setQuery(query) //
              .setFrom(from) //
              .setSize(MAX_SIZE) //
              .setNoFields();

          SearchResponse response = search.execute().actionGet();
          total = response.getHits().getTotalHits();
          for (SearchHit hit : response.getHits()) {
            DeleteRequestBuilder request = esSearchService.getClient().prepareDelete(getIndexName(), getIndexType(), hit.getId());
            if (hit.getFields() != null && hit.getFields().containsKey("_parent")) {
              String parent = hit.field("_parent").value();
              request.setParent(parent);
            }
            bulkRequest.add(request);
          }
          from = from + MAX_SIZE;
        }

        bulkRequest.execute().actionGet();
      } catch (Exception e) {
        //
      }

      cleanMapping();
    }

    @NotNull
    IndexMetaData createIndex() {
      // reset metadata cache
      esMapping = null;
      IndicesAdminClient idxAdmin = esSearchService.getClient().admin().indices();
      if (!idxAdmin.exists(new IndicesExistsRequest(getIndexName())).actionGet().isExists()) {
        log.info("Creating index [{}]", getIndexName());
        idxAdmin.prepareCreate(getIndexName()).setSettings(getIndexSettings()).execute().actionGet();
        createIndexWithMapping();
      } else {
        updateIndexWithMapping();
      }
      // reset metadata cache
      esMapping = null;
      return esSearchService.getClient().admin().cluster().prepareState().setIndices(getIndexName()).execute().actionGet()
          .getState().getMetaData().index(getIndexName());
    }

    private void createIndexWithMapping() {
      log.info("Creating index mapping [{}] for {}", getIndexName(), name);
      esSearchService.getClient().admin().indices().preparePutMapping(getIndexName()).setType(getIndexType())
          .setSource(createMapping()).execute().actionGet();
    }

    private void updateIndexWithMapping() {
      String indexName = getIndexName();
      log.info("Updating index mapping [{}] for {}", indexName, name);
      ESMapping mapping = readMapping();
      XContentBuilder newMapping = updateMapping(mapping);
      if (newMapping != null) {
        esSearchService.getClient().admin().indices().preparePutMapping(indexName).setType(getIndexType())
            .setSource(newMapping).execute().actionGet();
      }
    }

    /**
     * Create a full index mapping for the current table.
     *
     * @return
     */
    protected abstract XContentBuilder createMapping();

    /**
     * Update the index mapping properties for the current table.
     *
     * @param mapping
     */
    protected abstract XContentBuilder updateMapping(ESMapping mapping);

    @Override
    public boolean isUpToDate() {
      return Timestampeds.lastUpdateComparator.compare(this, resolveTable()) >= 0;
    }

    @Override
    public Timestamps getTimestamps() {
      return new Timestamps() {

        private final ESMapping.Meta meta;

        {
          meta = readMapping().meta();
        }

        @NotNull
        @Override
        public Value getLastUpdate() {
          return DateTimeType.get().valueOf(meta.getString(name));
        }

        @NotNull
        @Override
        public Value getCreated() {
          return DateTimeType.get().valueOf(meta.getString(name));
        }

      };
    }

    @NotNull
    protected String indexName(@NotNull ValueTable table) {
      String datasourceName = table.getDatasource().getName().replace(' ', '+').replace('.', '-');
      String tableName = table.getName().replace(' ', '_').replace('.', '-');
      return datasourceName + "__" + tableName;
    }

    @NotNull
    protected ESMapping readMapping() {
      if (esMapping != null) return esMapping;
      try {
        IndexMetaData indexMetaData = getIndexMetaData();
        if (indexMetaData != null) {
          MappingMetaData metaData = indexMetaData.mapping(getIndexType());
          if (metaData != null) {
            byte[] mappingSource = metaData.source().uncompressed();
            esMapping = new ESMapping(getIndexType(), mappingSource);
          }
        }
        if (esMapping == null) esMapping = new ESMapping(getIndexType());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return esMapping;
    }

    protected ValueTable resolveTable() {
      return MagmaEngineTableResolver.valueOf(valueTableReference).resolveTable();
    }

    private void cleanMapping() {
      try {
        ESMapping mapping = readMapping();
        if (mapping.meta().hasString(name)) {
          mapping.meta().deleteString(name);
          esSearchService.getClient().admin().indices().preparePutMapping(getIndexName()).setType(getIndexType())
              .setSource(mapping.toXContent()).execute().actionGet();
        }
        // reset metadata cache
        esMapping = null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Nullable
    private IndexMetaData getIndexMetaData() {
      if (esSearchService.getClient() == null) return null;
      return esSearchService.getClient().admin().cluster().prepareState().setIndices(getIndexName()).execute()
          .actionGet().getState().getMetaData().index(getIndexName());
    }

    @NotNull
    @Override
    public Calendar now() {
      Calendar c = Calendar.getInstance();
      c.setTime(new Date());
      return c;
    }

    @Override
    public int hashCode() {
      return getIndexType().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return obj != null && (obj == this ||
          obj instanceof ESValueTableIndex && ((ValueTableIndex) obj).getIndexType().equals(getIndexType()));
    }

  }

}
