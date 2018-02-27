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

import java.io.File;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import com.hortonworks.nifi.xml.GenerateJoltSpec;


public class GenerateJoltSpecTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        
    }

    @Test
    public void testArrayWtihArrayInXML() {
    	testRunner = TestRunners.newTestRunner(GenerateJoltSpec.class);
    	File file = new File("src/main/resources/schema.xsd");
        testRunner.setProperty(GenerateJoltSpec.XSD_SCHEMA, file.getAbsolutePath());
        testRunner.enqueue("<emp><id>1</id><addresses><addr><type>home</type></addr><addr><type>office</type></addr></addresses></emp>".getBytes());
        
        testRunner.run(1);
        
        List<MockFlowFile> outputFlowFile = testRunner.getFlowFilesForRelationship(GenerateJoltSpec.SUCCESS);
        
        String joltSpec = outputFlowFile.get(0).getAttribute("joltSpec");
        //System.out.println(joltSpec);
        assertTrue("[{\"operation\":\"modify-overwrite-beta\",\"spec\":{\"PurchaseOrder\":{\"ShipT*\":\"=toList\",\"ShipTo\":{\"name\":\"=toString\",\"street\":\"=toString\",\"city\":\"=toString\",\"stat*\":\"=toList\",\"state\":{\"stateName\":\"=toString\",\"stateCode\":\"=toString\"},\"zip\":\"=toInteger\"},\"BillTo\":{\"name\":\"=toString\",\"street\":\"=toString\",\"city\":\"=toString\",\"stat*\":\"=toList\",\"state\":{\"stateName\":\"=toString\",\"stateCode\":\"=toString\"},\"zip\":\"=toInteger\"},\"OrderDate\":\"=toString\"}}}]".equals(joltSpec));

        
    }
}

