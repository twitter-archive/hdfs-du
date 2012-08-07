/* 
 * Copyright 2012 Twitter, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.hdfsdu.pig.piggybank;

import com.google.common.collect.Maps;
import com.google.common.io.LineReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class TestExtractSizes {
  private static final Log LOG = LogFactory.getLog(TestExtractSizes.class);

  private static final TupleFactory tf = TupleFactory.getInstance();
  Tuple input_ = tf.newTuple();

  private ExtractSizes extractSizes = new ExtractSizes();

  @Test
  public final void testTeamENMatch() throws IOException, ExecException {
    InputStream inputStream = getClass().getResourceAsStream("/data.txt");
    InputStreamReader input = new InputStreamReader(inputStream);
    LineReader l = new LineReader(input);
    String line = l.readLine();
    while (line != null) {
      String[] entries = line.split("\t");
      Tuple testTuple = tf.newTuple();
      testTuple.append(entries[0]);
      System.out.println(Arrays.asList(entries));
      Long fileSize = Long.parseLong(entries[6]);
      testTuple.append(fileSize);

      if (fileSize != 0) {
        System.out.println(testTuple);
        System.out.println(extractSizes.exec(testTuple));
      }
      line = l.readLine();
    }
  }

  @Test
  public void testSizeByPath() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("INPUT", "src/test/resources/data.txt");
    params.put("OUTPUT", "target/test/data/hdfsdu.pig.out");
    PigServer server = new PigServer(ExecType.LOCAL);
    server.registerScript(getClass().getResource("/hdfsdu.pig").getPath(), params);

    Iterator<Tuple> it = server.openIterator("final_output");
    Tuple t = it.next();
    Assert.assertEquals("/", t.get(0));
    Assert.assertEquals(150L, t.get(1));
    Assert.assertEquals(3L, t.get(2));
    t = it.next();
    Assert.assertEquals("/jobs", t.get(0));
    Assert.assertEquals(50L, t.get(1));
    Assert.assertEquals(1L, t.get(2));
    Assert.assertNull(it.next());
  }
}
