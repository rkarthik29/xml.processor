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
package com.hortonworks.nifi.xml;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import de.odysseus.staxon.json.JsonXMLConfig;
import de.odysseus.staxon.json.JsonXMLConfigBuilder;
import de.odysseus.staxon.json.JsonXMLOutputFactory;
import de.odysseus.staxon.json.util.XMLMultipleEventWriter;

@Tags({"xml","json","convert"})
@CapabilityDescription("Convert XML to json")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class XML2JSON extends AbstractProcessor {

    public static final Relationship JSON = new Relationship.Builder()
            .name("JSON")
            .description("JSON Output")
            .build();
    
    public static final Relationship XML = new Relationship.Builder()
            .name("XML")
            .description("Originial XML ")
            .build();
    
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failure to convert")
            .build();
    
    public static final PropertyDescriptor AUTO_ARRAY = new PropertyDescriptor.Builder()
            .name("Automatically detect Arrays")
            .description("Detect if the XML occurs multiple times in the same root and make it an array in json")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("false")
            .build();
    
    public static final PropertyDescriptor INCLUDE_NS = new PropertyDescriptor.Builder()
            .name("Include Namesspace declaration in json")
            .description("Include Namesspace declaration in json")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("false")
            .build();
    
    public static final PropertyDescriptor AUTO_PRIMITIVE = new PropertyDescriptor.Builder()
            .name("Automatically detect primitives")
            .description("Automatically detect primitives, setting to false will make everything to be strings")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("false")
            .build();
    
    public static final PropertyDescriptor PRETTY_PRINT = new PropertyDescriptor.Builder()
            .name("Prretty print output json")
            .description("Prretty print output json")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("false")
            .build();
    
    public static final PropertyDescriptor ARRAY_ELEMENTS = new PropertyDescriptor.Builder()
            .name("comma seperated xpath for elements that may occur more than once within a root.")
            .description("comma seperated xpath for elements that may occur more than once within a root.")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor MATCHROOT = new PropertyDescriptor.Builder()
            .name("ensure multiple instances of array element occuring in the same parent node")
            .description("ensure multiple instances of array element occuring in the same parent node")
            .required(true)
            .defaultValue("false")
            .allowableValues("true","false")
            .build();
    
    private List<PropertyDescriptor> descriptors;
    
    private JsonXMLConfigBuilder builder;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(AUTO_PRIMITIVE);
    	descriptors.add(INCLUDE_NS);
    	descriptors.add(PRETTY_PRINT);
    	descriptors.add(AUTO_ARRAY);
    	descriptors.add(ARRAY_ELEMENTS);
    	descriptors.add(MATCHROOT);
        this.descriptors = Collections.unmodifiableList(descriptors);
        
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(JSON);
        //relationships.add(XML);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    	
    	builder = new JsonXMLConfigBuilder()
	            .autoArray(context.getProperty(AUTO_ARRAY).asBoolean())
	            .namespaceDeclarations(context.getProperty(INCLUDE_NS).asBoolean())
	            .autoPrimitive(context.getProperty(AUTO_PRIMITIVE).asBoolean())
	            .prettyPrint(context.getProperty(PRETTY_PRINT).asBoolean());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        String arrays = context.getProperty(ARRAY_ELEMENTS).getValue(); 
        boolean matchRoot =context.getProperty(MATCHROOT).asBoolean();
        JsonwriterCallback callback;
    	if(arrays==null || arrays.isEmpty()){
    		builder.multiplePI(false);
    		callback = new JsonwriterCallback(this.builder.build(),null,matchRoot);
    	}else{
    		builder.multiplePI(true);
    		callback = new JsonwriterCallback(this.builder.build(),arrays,matchRoot);
    	}
        
        // TODO imple
        FlowFile outputFlowFile = session.write(flowFile, callback);
        if(!callback.errors){
        	session.transfer(outputFlowFile,JSON);
        	//session.transfer(flowFile,XML);
        }else{
        	session.transfer(flowFile,FAILURE);
        }
    }
    
    private class JsonwriterCallback implements StreamCallback{
    	public boolean errors=false;
    	private JsonXMLConfig config;
    	private String arrays;
    	private boolean matchRoot;
    	public JsonwriterCallback(JsonXMLConfig config,String arrays,boolean matchRoot) {
			// TODO Auto-generated constructor stub
    		this.config=config;
    		this.arrays=arrays;
    		this.matchRoot=matchRoot;
		}
		@Override
		public void process(InputStream input, OutputStream output) throws IOException {
			// TODO Auto-generated method stub
		        try {
		            XMLEventReader reader = XMLInputFactory.newInstance().createXMLEventReader(input);
		            XMLEventWriter writer = new JsonXMLOutputFactory(config).createXMLEventWriter(output);
		            if(arrays!=null){
		            writer = new XMLMultipleEventWriter(writer,matchRoot, arrays.split(","));
		            }
		            writer.add(reader);
		            reader.close();
		            writer.close();
		        }catch(Exception e){
		        	errors=true;
		        } 
		}
    	
    }
}
