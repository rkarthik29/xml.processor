/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.nifi.xml.processor;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import com.hortonworks.nifi.xml.XML2JSON;


public class XML2JSONTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        
    }

    @Test
    public void testArrayWtihArrayInXML() {
    	testRunner = TestRunners.newTestRunner(XML2JSON.class);
        testRunner.setProperty(XML2JSON.AUTO_ARRAY, "true");
        testRunner.setProperty(XML2JSON.INCLUDE_NS, "false");
        testRunner.setProperty(XML2JSON.AUTO_PRIMITIVE, "true");
        testRunner.setProperty(XML2JSON.PRETTY_PRINT, "false");
        testRunner.setProperty(XML2JSON.ARRAY_ELEMENTS, "");
        testRunner.setProperty(XML2JSON.MATCHROOT, "false");
        testRunner.enqueue("<emp><id>1</id><addresses><addr><type>home</type></addr><addr><type>office</type></addr></addresses></emp>".getBytes());
        
        testRunner.run(1);
        
        List<MockFlowFile> outputFlowFile = testRunner.getFlowFilesForRelationship(XML2JSON.JSON);
        
        byte[] byteArray = outputFlowFile.get(0).toByteArray();
        String result = new String(byteArray);
        assertTrue("{\"emp\":{\"id\":1,\"addresses\":{\"addr\":[{\"type\":\"home\"},{\"type\":\"office\"}]}}}".equals(result));

        
    }
    @Test
    public void testArrayWtihNoArrayInXML() {
    	testRunner = TestRunners.newTestRunner(XML2JSON.class);
        testRunner.setProperty(XML2JSON.AUTO_ARRAY, "false");
        testRunner.setProperty(XML2JSON.INCLUDE_NS, "false");
        testRunner.setProperty(XML2JSON.AUTO_PRIMITIVE, "true");
        testRunner.setProperty(XML2JSON.PRETTY_PRINT, "false");
        testRunner.setProperty(XML2JSON.ARRAY_ELEMENTS, "/emp/addresses/addr");
        testRunner.setProperty(XML2JSON.MATCHROOT, "true");
        testRunner.enqueue("<emp><id>1</id><addresses><addr><type>home</type></addr></addresses></emp>".getBytes());
        
        testRunner.run(1);
        
        List<MockFlowFile> outputFlowFile = testRunner.getFlowFilesForRelationship(XML2JSON.JSON);
        
        byte[] byteArray = outputFlowFile.get(0).toByteArray();
        String result = new String(byteArray);
        System.out.println(result);
        assertTrue("{\"emp\":{\"id\":1,\"addresses\":{\"addr\":[{\"type\":\"home\"}]}}}".equals(result));

        
    }

}
