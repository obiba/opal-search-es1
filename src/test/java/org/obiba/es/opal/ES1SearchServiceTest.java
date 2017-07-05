/*
 * Copyright (c) 2017 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>. 
 */

package org.obiba.es.opal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obiba.core.util.FileUtil;
import org.obiba.opal.spi.ServicePlugin;
import org.obiba.opal.spi.vcf.VCFStoreService;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class ES1SearchServiceTest {

  @Before
  public void setUp() throws IOException {
    FileUtil.delete(new File(getDefaultProperties().getProperty(ServicePlugin.DATA_DIR_PROPERTY)));
    FileUtil.delete(new File(getDefaultProperties().getProperty(ServicePlugin.WORK_DIR_PROPERTY)));
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.delete(new File(getDefaultProperties().getProperty(ServicePlugin.DATA_DIR_PROPERTY)));
    FileUtil.delete(new File(getDefaultProperties().getProperty(ServicePlugin.WORK_DIR_PROPERTY)));
  }


  @Test
  public void testService() {
    ES1SearchService service = createService();
  }

  private ES1SearchService createService() {
    ES1SearchService service = new ES1SearchService();
    service.configure(getDefaultProperties());
    service.start();
    return service;
  }

  private Properties getDefaultProperties() {
    Properties properties = new Properties();
    properties.setProperty(ServicePlugin.DATA_DIR_PROPERTY, "target" + File.separator + "test-cluster-data");
    properties.setProperty(ServicePlugin.WORK_DIR_PROPERTY, "target" + File.separator + "test-cluster-work");
    properties.setProperty(ServicePlugin.INSTALL_DIR_PROPERTY, "src" + File.separator + "main" + File.separator + "conf");
    return properties;
  }

}
