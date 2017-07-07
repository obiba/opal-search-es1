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

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.obiba.es.opal.ESSearchService;
import org.obiba.opal.search.support.EsResultConverter;
import org.obiba.opal.search.support.QueryTermConverter;
import org.obiba.opal.search.support.ValueTableIndexManager;
import org.obiba.opal.spi.search.SearchQueryExecutor;
import org.obiba.opal.spi.search.ValueTableValuesIndex;
import org.obiba.opal.web.model.Search;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for executing an elastic search. The input and output of this class are DTO format.
 */
public class ESSearchQueryExecutor implements SearchQueryExecutor {

  private static final Logger log = LoggerFactory.getLogger(ESSearchQueryExecutor.class);

  private final ESSearchService esProvider;

  private final int termsFacetSizeLimit;

  private final ValueTableIndexManager valueTableIndexManager;

  public ESSearchQueryExecutor(ESSearchService esProvider, ValueTableIndexManager valueTableIndexManager, int termsFacetSizeLimit) {
    this.esProvider = esProvider;
    this.valueTableIndexManager = valueTableIndexManager;
    this.termsFacetSizeLimit = termsFacetSizeLimit;
  }

  /**
   * Executes an elastic search query.
   *
   * @param dtoQueries
   * @return
   * @throws JSONException
   */
  @Override
  public Search.QueryResultDto execute(Search.QueryTermsDto dtoQueries) throws JSONException {
    JSONObject jsonRequest = new JSONObject(build(dtoQueries, valueTableIndexManager));
    ValueTableValuesIndex valueTableValuesIndex = valueTableIndexManager.getValueTableValuesIndex();
    SearchRequestBuilder request = esProvider.getClient().prepareSearch()
        .setIndices(valueTableValuesIndex.getIndexName())
        .setTypes(valueTableValuesIndex.getIndexType())
        .setQuery(jsonRequest.getString("query"));
    if (jsonRequest.has("aggregations")) {
      request.setAggregations(jsonRequest.getJSONObject("aggregations").toString().getBytes());
    }
    if (jsonRequest.has("from"))
        request.setFrom(jsonRequest.getInt("from"))
        .setSize(jsonRequest.getInt("size"));
    // TODO sort
    if (jsonRequest.has("_source"))
        request.setSource(jsonRequest.getString("_source"));
    log.debug("request /{}/{} : {}", new String[] {valueTableValuesIndex.getIndexName(), valueTableValuesIndex.getIndexType(), request.toString()});
    SearchResponse response = request.execute().actionGet();
    JSONObject jsonContent = new JSONObject(response.toString());
    EsResultConverter converter = new EsResultConverter();
    return converter.convert(jsonContent);
  }

  /**
   * Executes a single elastic search query.
   *
   * @param dtoQuery
   * @return
   * @throws JSONException
   */
  @Override
  public Search.QueryResultDto execute(Search.QueryTermDto dtoQuery) throws JSONException {
    // wrap in a QueryTermsDto for API uniformity
    Search.QueryTermsDto dtoQueries = Search.QueryTermsDto.newBuilder().addQueries(dtoQuery).build();
    return execute(dtoQueries);
  }

  private String build(Search.QueryTermsDto dtoQueries, ValueTableIndexManager valueTableIndexManager) throws JSONException {
    QueryTermConverter converter = new QueryTermConverter(valueTableIndexManager, termsFacetSizeLimit);
    JSONObject queryJSON = converter.convert(dtoQueries);

    return queryJSON.toString();
  }
}