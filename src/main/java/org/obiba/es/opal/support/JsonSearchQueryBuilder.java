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

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.obiba.opal.spi.search.QuerySettings;

public class JsonSearchQueryBuilder {

  private QuerySettings querySettings;

  private JsonSearchQueryBuilder(QuerySettings settings) {
    querySettings = settings;
  }

  //
  // Public methods
  //

  public static JsonSearchQueryBuilder newSearchQuery(QuerySettings settings) {
    JsonSearchQueryBuilder builder = new JsonSearchQueryBuilder(settings);
    return builder;
  }

  public JSONObject build() throws JSONException {
    JSONObject jsonQuery = new JSONObject();
    if (querySettings.hasChildQueries())
      jsonQuery.put("query", buildHasChildQueries());
    else
      jsonQuery.put("query", buildQueryString(querySettings.getQuery(), querySettings.withDefaultFields()));
    jsonQuery.put("sort", buildSortJson());
    if (querySettings.hasFields()) jsonQuery.put("_source", buildFields());
    if (querySettings.hasFilterReferences()) jsonQuery.put("filter", buildFilter());
    jsonQuery.put("from", querySettings.getFrom());
    jsonQuery.put("size", querySettings.getSize());
    if (querySettings.hasFacets()) jsonQuery.put("facets", buildFacetsJson());

    return jsonQuery;
  }

  //
  // Private members
  //

  private JSONObject buildQueryString(String query, boolean defaultFields) throws JSONException {
    JSONObject json = new JSONObject();
    if (defaultFields && !querySettings.hasFacets() && !"*".equals(query))
      json.put("fields", new JSONArray(querySettings.defaultQueryFields));
    json.put("query", query);
    json.put("default_operator", querySettings.DEFAULT_QUERY_OPERATOR);
    return new JSONObject().put("query_string", json);
  }

  private JSONObject buildHasChildQueries() throws JSONException {
    JSONObject json = new JSONObject();
    for (QuerySettings.ChildQuery child : querySettings.getChildQueries())
      json.accumulate(querySettings.getChildQueryOperator(), buildHasChildQuery(child));
    return new JSONObject().put("bool", json);
  }

  private JSONObject buildHasChildQuery(QuerySettings.ChildQuery child) throws JSONException {
    JSONObject json = new JSONObject();
    json.put("type", child.getType());
    json.put("query", buildQueryString(child.getQuery(), false));
    return new JSONObject().put("has_child", json);
  }

  private JSONObject buildSortJson() throws JSONException {
    if (!querySettings.hasSortField()) {
      return new JSONObject();
    }

    return new JSONObject().put(querySettings.getSortField(), new JSONObject().put("order", querySettings.getSortDir()));
  }

  private JSONObject buildFilter() throws JSONException {
    return new JSONObject().put("terms", new JSONObject().put("reference", new JSONArray(querySettings.getFilterReferences())));
  }

  private JSONObject buildFacetsJson() throws JSONException {
    JSONObject jsonFacets = new JSONObject();
    for (String facet : querySettings.getFacets()) {
      String field = facet;
      int size = 10;
      int idx = facet.lastIndexOf(":");
      if (idx > 0) {
        field = facet.substring(0, idx);
        try {
          size = Integer.parseInt(facet.substring(idx + 1));
        } catch (NumberFormatException e) {
          // ignore
        }
      }
      jsonFacets.accumulate(facet, new JSONObject().put("terms", new JSONObject().put("field", field).put("size", size)));
    }

    return jsonFacets;
  }

  private JSONObject buildFields() throws JSONException {
    return new JSONObject().put("includes", new JSONArray(querySettings.getFields()));
  }
}
