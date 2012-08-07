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

package com.twitter.hdfsdu;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.json.simple.JSONObject;

import com.google.common.io.LineReader;
import com.twitter.hdfsdu.data.DataTransformer;
import com.twitter.hdfsdu.data.NodeData;

public class DataTransformerTest  extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public DataTransformerTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( DataTransformerTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testDataTransformer() throws Exception
    {
    	//load file
        InputStream inputStream = DataTransformerTest.class.getClassLoader().getResourceAsStream("com/twitter/hdfsdu/data/example.txt");
        InputStreamReader input = new InputStreamReader(inputStream);
        LineReader l = new LineReader(input);
        String line = l.readLine();
        List<NodeData> nlist = new ArrayList<NodeData>();
        while (line != null) {
          String[] entries = line.split("\t");
          NodeData n = new NodeData();
          n.path = entries[0];
          n.fileSize = entries[1];
          nlist.add(n);
          line = l.readLine();
        }
        JSONObject ans = DataTransformer.getJSONTree("/", 2, nlist);
        System.out.println(ans.toJSONString());
//        assertTrue( true );
    }
}
