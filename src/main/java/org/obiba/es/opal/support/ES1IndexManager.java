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
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.TypeMissingException;
import org.obiba.es.opal.ES1SearchService;
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

public abstract class ES1IndexManager implements IndexManager {

  private static final Logger log = LoggerFactory.getLogger(ES1IndexManager.class);

  protected static final int ES_BATCH_SIZE = 100;

  protected final ES1SearchService es1SearchService;

  private final Map<String, ValueTableIndex> indices = Maps.newHashMap();

  protected ES1IndexManager(ES1SearchService es1SearchService) {
    this.es1SearchService = es1SearchService;
  }

  @Override
  public ValueTableIndex getIndex(@NotNull ValueTable vt) {
    String tableFullName = vt.getTableReference();
    ValueTableIndex index = indices.computeIfAbsent(tableFullName, k -> createIndex(vt));
    return index;
  }

  @Override
  public boolean hasIndex(@NotNull ValueTable valueTable) {
    ClusterStateResponse resp = es1SearchService.getClient().admin().cluster().prepareState().execute().actionGet();
    ImmutableOpenMap<String, MappingMetaData> mappings = resp.getState().metaData().index(getName()).mappings();
    return mappings.containsKey(getIndex(valueTable).getIndexType());

  }

  protected abstract ValueTableIndex createIndex(@NotNull ValueTable vt);

  @Override
  public boolean isEnabled() {
    return es1SearchService.getConfig().isEnabled();
  }

  @Override
  public boolean isReady() {
    return es1SearchService.isEnabled() && es1SearchService.getConfig().isEnabled();
  }

  @Override
  public boolean isIndexUpToDate(@NotNull ValueTable valueTable) {
    return getIndex(valueTable).isUpToDate();
  }

  protected String esIndexName() {
    return es1SearchService.getConfig().getIndexName();
  }

  protected Settings getIndexSettings() {
    return ImmutableSettings.settingsBuilder() //
        .put("number_of_shards", es1SearchService.getConfig().getShards()) //
        .put("number_of_replicas", es1SearchService.getConfig().getReplicas()).build();
  }

  @NotNull
  protected IndexMetaData createIndex() {
    IndicesAdminClient idxAdmin = es1SearchService.getClient().admin().indices();
    if(!idxAdmin.exists(new IndicesExistsRequest(getName())).actionGet().isExists()) {
      log.info("Creating index [{}]", getName());
      idxAdmin.prepareCreate(getName()).setSettings(getIndexSettings()).execute().actionGet();
    }
    return es1SearchService.getClient().admin().cluster().prepareState().setIndices(getName()).execute().actionGet()
        .getState().getMetaData().index(getName());
  }

  protected abstract class ES1Indexer implements IndexSynchronization {

    @NotNull
    protected final ValueTable valueTable;

    @NotNull
    private final ES1ValueTableIndex index;

    private final int total;

    protected int done = 0;

    protected boolean stop = false;

    protected ES1Indexer(@NotNull ValueTable table, @NotNull ES1ValueTableIndex index) {
      valueTable = table;
      this.index = index;
      total = valueTable.getVariableEntityCount();
    }

    @Override
    public IndexManager getIndexManager() {
      return ES1IndexManager.this;
    }

    @Override
    public void run() {
      log.debug("Updating ValueTable index {}", index.getValueTableReference());
      index.delete();
      createIndex();
      index.createMapping();
      index();
    }

    protected BulkRequestBuilder sendAndCheck(BulkRequestBuilder bulkRequest) {
      if(bulkRequest.numberOfActions() > 0) {
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if(bulkResponse.hasFailures()) {
          // process failures by iterating through each bulk response item
          throw new RuntimeException(bulkResponse.buildFailureMessage());
        }
        return es1SearchService.getClient().prepareBulk();
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

  protected abstract class ES1ValueTableIndex implements ValueTableIndex {

    @NotNull
    private final String name;

    @NotNull
    private final String valueTableReference;

    private boolean mappingCreated;

    /**
     * @param vt
     */
    protected ES1ValueTableIndex(@NotNull ValueTable vt) {
      name = indexName(vt);
      valueTableReference = vt.getTableReference();
    }

    @NotNull
    @Override
    public String getIndexType() {
      return name;
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
        ES1Mapping mapping = readMapping();
        //noinspection ConstantConditions
        mapping.meta().setString("_updated", DateTimeType.get().valueOf(new Date()).toString());
        es1SearchService.getClient().admin().indices().preparePutMapping(getName()).setType(getIndexType())
            .setSource(mapping.toXContent()).execute().actionGet();
      } catch(IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void delete() {
      if(es1SearchService.isEnabled() && es1SearchService.isRunning()) {
        try {
          es1SearchService.getClient().admin().indices().prepareDeleteMapping(getName()).setType(getIndexType())
              .execute().actionGet();
        } catch(TypeMissingException | IndexMissingException ignored) {
        } finally {
          mappingCreated = false;
        }
      }
    }

    protected void createMapping() {
      if(mappingCreated) return;
      getIndexMetaData(); // create index if it does not exist yet
      es1SearchService.getClient().admin().indices().preparePutMapping(getName()).setType(getIndexType())
          .setSource(getMapping()).execute().actionGet();
      mappingCreated = true;
    }

    protected abstract XContentBuilder getMapping();

    @Override
    public boolean isUpToDate() {
      return Timestampeds.lastUpdateComparator.compare(this, resolveTable()) >= 0;
    }

    @Override
    public Timestamps getTimestamps() {
      return new Timestamps() {

        private final ES1Mapping.Meta meta;

        {
          meta = readMapping().meta();
        }

        @NotNull
        @Override
        public Value getLastUpdate() {
          return DateTimeType.get().valueOf(meta.getString("_updated"));
        }

        @NotNull
        @Override
        public Value getCreated() {
          return DateTimeType.get().valueOf(meta.getString("_created"));
        }

      };
    }

    @NotNull
    private String indexName(@NotNull ValueTable table) {
      String datasourceName = table.getDatasource().getName().replace(' ', '+').replace('.', '-');
      String tableName = table.getName().replace(' ', '_').replace('.', '-');
      return datasourceName + "__" + tableName;
    }

    @NotNull
    protected ES1Mapping readMapping() {
      try {
        try {
          IndexMetaData indexMetaData = getIndexMetaData();

          if(indexMetaData != null) {
            MappingMetaData metaData = indexMetaData.mapping(getIndexType());
            if(metaData != null) {
              byte[] mappingSource = metaData.source().uncompressed();
              return new ES1Mapping(getIndexType(), mappingSource);
            }
          }

          mappingCreated = false;
          return new ES1Mapping(getIndexType());
        } catch(IndexMissingException e) {
          mappingCreated = false;
          return new ES1Mapping(getIndexType());
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
      if(es1SearchService.getClient() == null) return null;

      IndexMetaData imd = es1SearchService.getClient().admin().cluster().prepareState().setIndices(getName()).execute()
          .actionGet().getState().getMetaData().index(getName());
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
          obj instanceof ES1ValueTableIndex && ((ValueTableIndex) obj).getIndexType().equals(getIndexType()));
    }

  }

}
