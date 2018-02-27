package com.hortonworks.nifi.xml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.hortonworks.nifi.xml.util.XSD2JOLT;

import de.odysseus.staxon.json.JsonXMLConfigBuilder;

@Tags({"xml","json","convert"})
@CapabilityDescription("Convert XML to json")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="joltSpec", description="")})
public class GenerateJoltSpec extends AbstractProcessor {
	
	public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("SUCCESS Output")
            .build();
    
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failure to convert")
            .build();
    
	public static final PropertyDescriptor XSD_SCHEMA = new PropertyDescriptor.Builder()
            .name("XSD SChema to generate spec from")
            .description("XSD SChema to generate spec from")
            .required(true)
            .defaultValue("")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    
    private List<PropertyDescriptor> descriptors;
    
    private JsonXMLConfigBuilder builder;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        
    	descriptors.add(XSD_SCHEMA);
        this.descriptors = Collections.unmodifiableList(descriptors);
        
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
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
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		// TODO Auto-generated method stub
		FlowFile flowFile=session.get();
		if(flowFile==null)
			return;
		try{
		FileInputStream fis = new FileInputStream(new File(context.getProperty(XSD_SCHEMA).getValue()));
		String spec = XSD2JOLT.xsd2joltspec(fis);
		flowFile = session.putAttribute(flowFile, "joltSpec", spec);
		session.transfer(flowFile, SUCCESS);
		}catch(Exception ex){
			ex.printStackTrace(System.out);
			session.transfer(flowFile, FAILURE);
			throw new ProcessException(ex.getMessage());
		}
	}

}
