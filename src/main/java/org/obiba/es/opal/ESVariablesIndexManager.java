/*
 * Copyright (c) 2018 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.obiba.es.opal;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.obiba.es.opal.mapping.AttributeMapping;
import org.obiba.es.opal.mapping.ValueTableVariablesMapping;
import org.obiba.es.opal.support.ESIndexManager;
import org.obiba.es.opal.support.ESMapping;
import org.obiba.magma.*;
import org.obiba.magma.support.VariableNature;
import org.obiba.opal.spi.search.IndexSynchronization;
import org.obiba.opal.spi.search.ValueTableIndex;
import org.obiba.opal.spi.search.ValueTableVariablesIndex;
import org.obiba.opal.spi.search.VariablesIndexManager;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ESVariablesIndexManager extends ESIndexManager implements VariablesIndexManager {

  protected ESVariablesIndexManager(ESSearchService esSearchService) {
    super(esSearchService);
  }

//  private static final Logger log = LoggerFactory.getLogger(EsVariablesIndexManager.class);

  @NotNull
  @Override
  public ESValueTableVariablesIndex getIndex(@NotNull ValueTable vt) {
    return (ESValueTableVariablesIndex) super.getIndex(vt);
  }

  @Override
  protected ValueTableIndex createIndex(@NotNull ValueTable vt) {
    return new ESValueTableVariablesIndex(vt);
  }

  @Override
  public boolean isReady() {
    return esSearchService.isEnabled();
  }

  @NotNull
  @Override
  public IndexSynchronization createSyncTask(ValueTable valueTable, ValueTableIndex index) {
    return new Indexer(valueTable, (ESValueTableVariablesIndex) index);
  }

  @NotNull
  @Override
  public String getName() {
    return esIndexName() + "-variables";
  }

  private class Indexer extends ESIndexer {

    private final ESValueTableVariablesIndex index;

    private Indexer(ValueTable table, ESValueTableVariablesIndex index) {
      super(table, index);
      this.index = index;
    }

    @Override
    protected void index() {
      BulkRequestBuilder bulkRequest = esSearchService.getClient().prepareBulk();

      for(Variable variable : valueTable.getVariables()) {
        bulkRequest = indexVariable(variable, bulkRequest);
      }

      sendAndCheck(bulkRequest);
      index.updateTimestamps();
    }

    private BulkRequestBuilder indexVariable(Variable variable, BulkRequestBuilder bulkRequest) {
      String fullName = valueTable.getDatasource().getName() + "." + valueTable.getName() + ":" + variable.getName();
      try {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject();
        xcb.field("project", valueTable.getDatasource().getName());
        xcb.field("datasource", valueTable.getDatasource().getName());
        xcb.field("table", valueTable.getName());
        xcb.field("reference", valueTable.getTableReference());
        xcb.field("fullName", fullName);
        indexVariableParameters(variable, xcb);

        if(variable.hasAttributes()) {
          indexVariableAttributes(variable, xcb);
        }

        if(variable.hasCategories()) {
          indexVariableCategories(variable, xcb);
        }

        bulkRequest.add(esSearchService.getClient().prepareIndex(index.getIndexName(), index.getIndexType(), fullName)
            .setSource(xcb.endObject()));
        if(bulkRequest.numberOfActions() >= ES_BATCH_SIZE) {
          return sendAndCheck(bulkRequest);
        }
      } catch(IOException e) {
        throw new RuntimeException(e);
      }
      return bulkRequest;
    }

    private void indexVariableParameters(Variable variable, XContentBuilder xcb) throws IOException {
      xcb.field("name", variable.getName());
      xcb.field("entityType", variable.getEntityType());
      xcb.field("valueType", variable.getValueType().getName());
      xcb.field("occurrenceGroup", variable.getOccurrenceGroup());
      xcb.field("repeatable", variable.isRepeatable());
      xcb.field("mimeType", variable.getMimeType());
      xcb.field("unit", variable.getUnit());
      xcb.field("referencedEntityType", variable.getReferencedEntityType());
      xcb.field("nature", VariableNature.getNature(variable).name());
      xcb.field("index", variable.getIndex());
    }

    private void indexVariableAttributes(AttributeAware variable, XContentBuilder xcb) throws IOException {
      for(Attribute attribute : variable.getAttributes()) {
        if(!attribute.getValue().isNull()) {
          xcb.field(AttributeMapping.getFieldName(attribute), attribute.getValue());
        }
      }
    }

    private void indexVariableCategories(Variable variable, XContentBuilder xcb) throws IOException {
      List<String> names = Lists.newArrayList();
      Map<String, List<Object>> attributeFields = Maps.newHashMap();
      for(Category category : variable.getCategories()) {
        names.add(category.getName());
        if(category.hasAttributes()) {
          for(Attribute attribute : category.getAttributes()) {
            String field = "category-" + AttributeMapping.getFieldName(attribute);
            if(!attributeFields.containsKey(field)) {
              attributeFields.put(field, new ArrayList<>());
            }
            Value value = attribute.getValue();
            attributeFields.get(field).add(value.isNull() ? null : value.getValue());
          }
        }
      }
      xcb.field("category", names);
      for(String field : attributeFields.keySet()) {
        xcb.field(field, attributeFields.get(field));
      }
    }
  }

  private class ESValueTableVariablesIndex extends ESValueTableIndex implements ValueTableVariablesIndex {

    private ESValueTableVariablesIndex(ValueTable vt) {
      super(vt);
    }

    @Override
    public String getIndexType() {
      return "Variable";
    }

    @Override
    protected XContentBuilder createMapping() {
      return new ValueTableVariablesMapping().createMapping(getIndexType(), esSearchService.getConfig().getLocales());
    }

    @Override
    protected XContentBuilder updateMapping(ESMapping mapping) {
      // ignore: attributes are strings
      return null;
    }

    @NotNull
    @Override
    public String getFieldName(@NotNull Attribute attribute) {
      return AttributeMapping.getFieldName(attribute);
    }
  }
}
