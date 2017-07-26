/*
 * Copyright (c) 2017 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.obiba.es.opal.mapping;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.obiba.magma.ValueTable;
import org.obiba.magma.Variable;
import org.obiba.magma.type.DateTimeType;

import java.io.IOException;
import java.util.Date;

public class ValueTableMapping {

  private final VariableMappings variableMappings = new VariableMappings();

  public XContentBuilder createMapping(String indexType, ValueTable valueTable) {
    try {
      XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject(indexType);
      mapping.startObject("_all").field("enabled", false).endObject();
      mapping.startObject("_parent").field("type", valueTable.getEntityType()).endObject();

      mapping.startObject("properties");

      MappingHelper.mapAnalyzedString("identifier", mapping);
      MappingHelper.mapNotAnalyzedString("project", mapping);
      MappingHelper.mapNotAnalyzedString("datasource", mapping);
      MappingHelper.mapNotAnalyzedString("table", mapping);
      MappingHelper.mapNotAnalyzedString("reference", mapping);

      for(Variable variable : valueTable.getVariables()) {
        variableMappings.map(valueTable.getTableReference(), variable, mapping);
      }

      mapping.endObject();// properties

      mapping.startObject("_meta") //
          .field("_updated", DateTimeType.get().valueOf(new Date()).toString()) //
          .endObject();

      mapping.endObject() // type
          .endObject(); // mapping
      return mapping;
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }

}
