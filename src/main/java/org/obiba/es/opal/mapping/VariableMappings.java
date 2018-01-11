/*
 * Copyright (c) 2018 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.obiba.es.opal.mapping;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.obiba.es.opal.support.ESMapping;
import org.obiba.magma.ValueTable;
import org.obiba.magma.Variable;
import org.obiba.magma.type.TextType;
import org.obiba.opal.spi.search.ValuesIndexManager;

import java.io.IOException;
import java.util.Map;

public class VariableMappings {

  private final ValueTypeMappings valueTypeMappings = new ValueTypeMappings();

  private final Iterable<VariableMapping> mappings = ImmutableList.of(new CategoricalMapping());

  public void map(ValueTable table, Variable variable, XContentBuilder builder) {
    try {
      String fieldName = MappingHelper.toFieldName(table.getTableReference(), variable);
      builder.startObject(fieldName);
      valueTypeMappings.forType(variable.getValueType()).map(builder);
      for(VariableMapping variableMapping : mappings)
        variableMapping.map(variable, builder);
      builder.endObject();
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void map(ValueTable table, Variable variable, ESMapping mapping) {
    String fieldName = MappingHelper.toFieldName(table.getTableReference(), variable);
    if(!mapping.properties().hasProperty(fieldName)) {
      Map<String, Object> fieldMapping = Maps.newHashMap();
      valueTypeMappings.forType(variable.getValueType()).map(fieldMapping);
      for(VariableMapping variableMapping : mappings)
        variableMapping.map(variable, fieldMapping);
      mapping.properties().setProperty(fieldName, fieldMapping);
    }
  }

  /**
   * Used to prevent Lucene analyzers from running on categorical values
   */
  private static class CategoricalMapping implements VariableMapping {

    @Override
    public void map(Variable variable, XContentBuilder builder) throws IOException {
      if(variable.hasCategories() && TextType.get().equals(variable.getValueType())) {
        builder.field("index", "not_analyzed");
      }
    }

    @Override
    public void map(Variable variable, Map<String, Object> mapping) {
      if(variable.hasCategories() && TextType.get().equals(variable.getValueType())) {
        mapping.put("index", "not_analyzed");
      }
    }
  }
}
