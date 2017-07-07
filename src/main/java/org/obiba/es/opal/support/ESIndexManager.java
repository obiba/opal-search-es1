/*
 * Copyright (c) 2017 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.obiba.es.opal.support;

import com.google.common.collect.Maps;
import org.apache.lucene.index.IndexNotFoundException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.TypeMissingException;
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

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

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
    ImmutableOpenMap<String, MappingMetaData> mappings = resp.getState().metaData().index(valueTableIndex.getIndexName()).getMappings();
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
  public boolean isIndexUpToDate(@NotNull ValueTable valueTable) {
    return getIndex(valueTable).isUpToDate();
  }

  protected String esIndexName() {
    return esSearchService.getConfig().getIndexName();
  }

  protected Settings getIndexSettings() {
    return Settings.settingsBuilder() //
        .put("number_of_shards", esSearchService.getConfig().getShards()) //
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
      //index.createMapping();
      index();
    }

    protected BulkRequestBuilder sendAndCheck(BulkRequestBuilder bulkRequest) {
      if(bulkRequest.numberOfActions() > 0) {
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if(bulkResponse.hasFailures()) {
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

    @NotNull
    private final String name;

    @NotNull
    private final String valueTableReference;


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
        ESMapping mapping = readMapping();
        //noinspection ConstantConditions
        mapping.meta().setString(name, DateTimeType.get().valueOf(new Date()).toString());
        esSearchService.getClient().admin().indices().preparePutMapping(getIndexName()).setType(getIndexType())
            .setSource(mapping.toXContent()).execute().actionGet();
      } catch(IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void delete() {
      if(esSearchService.isEnabled() && esSearchService.isRunning()) {
        try {
          esSearchService.getClient().admin().indices().prepareDelete(getIndexName()).execute().actionGet();
        } catch(TypeMissingException ignored) {
        }
      }
    }

    @NotNull
    IndexMetaData createIndex() {
      IndicesAdminClient idxAdmin = esSearchService.getClient().admin().indices();
      if(!idxAdmin.exists(new IndicesExistsRequest(getIndexName())).actionGet().isExists()) {
        log.info("Creating index [{}]", getIndexName());
        idxAdmin.prepareCreate(getIndexName()).setSettings(getIndexSettings()).execute().actionGet();
        createMapping();
      }
      return esSearchService.getClient().admin().cluster().prepareState().setIndices(getIndexName()).execute().actionGet()
          .getState().getMetaData().index(getIndexName());
    }

    private void createMapping() {
      esSearchService.getClient().admin().indices().preparePutMapping(getIndexName()).setType(getIndexType())
          .setSource(getMapping()).execute().actionGet();
    }

    protected abstract XContentBuilder getMapping();

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
          return DateTimeType.get().valueOf(meta.getString("_created"));
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
      try {
        try {
          IndexMetaData indexMetaData = getIndexMetaData();
          if(indexMetaData != null) {
            MappingMetaData metaData = indexMetaData.mapping(getIndexType());
            if(metaData != null) {
              byte[] mappingSource = metaData.source().uncompressed();
              return new ESMapping(getIndexType(), mappingSource);
            }
          }
          return new ESMapping(getIndexType());
        } catch(IndexNotFoundException e) {
          return new ESMapping(getIndexType());
        }
      } catch(IOException e) {
        throw new RuntimeException(e);
      }
    }

    protected ValueTable resolveTable() {
      return MagmaEngineTableResolver.valueOf(valueTableReference).resolveTable();
    }

    @Nullable
    private IndexMetaData getIndexMetaData() {
      if(esSearchService.getClient() == null) return null;

      IndexMetaData imd = esSearchService.getClient().admin().cluster().prepareState().setIndices(getIndexName()).execute()
          .actionGet().getState().getMetaData().index(getIndexName());
      return imd == null ? createIndex() : imd;
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
