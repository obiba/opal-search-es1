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

import com.google.common.collect.Iterators;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.sort.SortOrder;
import org.obiba.es.opal.ESSearchService;
import org.obiba.opal.spi.search.QuerySettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESQueryExecutor {

  private static final Logger log = LoggerFactory.getLogger(ESQueryExecutor.class);

  private final ESSearchService esSearchService;

  private String searchPath;

  public ESQueryExecutor(ESSearchService esSearchService) {
    this.esSearchService = esSearchService;
  }

  public ESQueryExecutor setSearchPath(String path) {
    searchPath = path;
    return this;
  }

  public JSONObject execute(QuerySettings querySettings) throws JSONException {
    // TODO make a SearchRequestBuilder instead of a JSON object
    return execute(JsonSearchQueryBuilder.newSearchQuery(querySettings).build());
  }

  private JSONObject execute(JSONObject jsonRequest) throws JSONException {
    if (log.isTraceEnabled()) log.trace("Request: " + searchPath + " => " + jsonRequest.toString(2));
    String[] parts = searchPath.split("/");

    SearchRequestBuilder request = esSearchService.getClient().prepareSearch()
        .setIndices(parts[0])
        .setQuery(jsonRequest.getString("query"))
        .setFrom(jsonRequest.getInt("from"))
        .setSize(jsonRequest.getInt("size"));

    if (jsonRequest.has("sort")) {
      JSONArray sort = jsonRequest.getJSONArray("sort");
      try {
        for (int i=0; i<sort.length(); i++) {
          JSONObject sortObject = sort.getJSONObject(i);
          String key = sortObject.keys().next().toString();
          String order = sortObject.getJSONObject(key).getString("order");
          request.addSort(key, SortOrder.valueOf(order.toUpperCase()));
        }
      } catch (Exception e) {
        log.warn("Unable to interpret the sort object: " + sort.toString());
      }
    }
    if (jsonRequest.has("filter")) {
      request.setPostFilter(jsonRequest.getString("filter"));
    }
    if (jsonRequest.has("_source")) {
      JSONArray jsonInclude = jsonRequest.getJSONArray("_source");
      String[] include = new String[jsonInclude.length()];
      for (int i = 0; i < jsonInclude.length(); i++) include[i] = jsonInclude.getString(i);
      request.setFetchSource(include, new String[0]);
    }
    if (parts.length > 1) request.setTypes(parts[1]);
    log.debug("request /{} : {}", searchPath, request.toString());
    SearchResponse response = request.execute().actionGet();
    JSONObject jsonResponse = new JSONObject(response.toString());
    return jsonResponse;
  }
}
