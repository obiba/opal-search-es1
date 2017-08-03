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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ESMapping {

  private final String name;

  private final Map<String, Object> mapping;

  public ESMapping(String name, byte... mappingSource) throws IOException {
    this.name = name;
    mapping = XContentFactory.xContent(mappingSource).createParser(mappingSource).map();
  }

  public ESMapping(String name) throws IOException {
    this.name = name;
    mapping = Maps.newHashMap();
  }

  public XContentBuilder toXContent() throws IOException {
    return JsonXContent.contentBuilder().map(mapping);
  }

  public Meta meta() {
    return new Meta();
  }

  public Properties properties() {
    return new Properties();
  }

  private Map<String, Object> type() {
    return newIfAbsent(mapping, name);
  }

  @SuppressWarnings("ParameterHidesMemberVariable")
  public class Meta {

    public boolean hasString(String name) {
      return meta().containsKey(name);
    }

    public String getString(String name) {
      return (String) meta().get(name);
    }

    public Meta setString(String name, String value) {
      meta().put(name, value);
      return this;
    }

    public Meta deleteString(String name) {
      meta().remove(name);
      return this;
    }

    private Map<String, Object> meta() {
      return newIfAbsent(type(), "_meta");
    }
  }

  @SuppressWarnings("ParameterHidesMemberVariable, unchecked")
  public class Properties {

    public boolean hasProperty(String name) {
      return properties().containsKey(name);
    }

    public void removeProperty(String name) {
      properties().remove(name);
    }

    public void removeProperties(String namePrefix) {
      Map<String, Object> props = properties();
      List<String> fields = props.keySet().stream().filter(k -> k.startsWith(namePrefix)).collect(Collectors.toList());
      fields.forEach(props::remove);
    }

    public Map<String, Object> getProperty(String name) {
      return (Map<String, Object>)properties().get(name);
    }

    private Map<String, Object> properties() {
      return newIfAbsent(type(), "properties");
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> newIfAbsent(Map<String, Object> map, String key) {
    Map<String, Object> inner = (Map<String, Object>) map.get(key);
    if(inner == null) {
      inner = Maps.newHashMap();
      map.put(key, inner);
    }
    return inner;
  }

}
