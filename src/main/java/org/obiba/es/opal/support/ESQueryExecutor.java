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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESQueryExecutor {

  private static final Logger log = LoggerFactory.getLogger(ESQueryExecutor.class);

  private final ESSearchService elasticSearchProvider;

  private String searchPath;

  public ESQueryExecutor(ESSearchService elasticSearchProvider) {
    this.elasticSearchProvider = elasticSearchProvider;
  }

  public ESQueryExecutor setSearchPath(String path) {
    searchPath = path;
    return this;
  }

  public JSONObject execute(JSONObject jsonRequest) throws JSONException {
    if (log.isDebugEnabled()) log.debug("Request: " + searchPath + " => " + jsonRequest.toString(2));
    SearchRequestBuilder request = elasticSearchProvider.getClient().prepareSearch()
        .setIndices(searchPath)
        .setQuery(jsonRequest.getString("query"))
        .setFrom(jsonRequest.getInt("from"))
        .setSize(jsonRequest.getInt("size"));
    // TODO sort
    if (jsonRequest.has("_source"))
      request.setSource(jsonRequest.getString("_source"));
    SearchResponse response = request.execute().actionGet();
    JSONObject jsonResponse = new JSONObject(response.toString());
    return jsonResponse;
  }
}
