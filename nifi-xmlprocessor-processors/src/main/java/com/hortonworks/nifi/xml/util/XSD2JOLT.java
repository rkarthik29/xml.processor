package com.hortonworks.nifi.xml.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class XSD2JOLT {
	static HashMap<String, List<JsonObject>> compMap= new HashMap<String,List<JsonObject>>();
	
	public static String xsd2joltspec(InputStream fis) throws FactoryConfigurationError, NumberFormatException, Exception{
		//InputStream fis = new FileInputStream(is);
		XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(fis);
		JsonArray spec = new JsonArray();
		JsonObject specDetails = new JsonObject();
		spec.add(specDetails);
		specDetails.addProperty("operation", "modify-overwrite-beta");
		JsonObject joltObj = new JsonObject();
		specDetails.add("spec", joltObj);
		while(reader.hasNext()){
			switch(reader.next()){
			case XMLEvent.START_ELEMENT:
				
				String type=reader.getAttributeValue("","type")==null?"":reader.getAttributeValue("","type");
				if(reader.getLocalName().equals("element")){
				Integer maxOccurs = Integer.parseInt(reader.getAttributeValue("","maxOccurs")==null?"0":reader.getAttributeValue("","maxOccurs"));
				String name=reader.getAttributeValue("","name");
				
				if(type.contains("int")||type.contains("short")){
					if(maxOccurs > 1 || maxOccurs<0){
						joltObj.addProperty(name, "=toList");
					}else
					joltObj.addProperty(reader.getAttributeValue("","name"), "=toInteger");
				}else if(type.contains("decimal")){
					if(maxOccurs > 1 || maxOccurs<0){
						joltObj.addProperty(name, "=toList");
					}else
					joltObj.addProperty(reader.getAttributeValue("","name"), "=toDouble");
				}else if(type.contains("long")){
					if(maxOccurs > 1 || maxOccurs<0){
						joltObj.addProperty(name, "=toList");
					}else
					joltObj.addProperty(reader.getAttributeValue("","name"), "=toLong");
				}else if(type.contains("string")||type.contains("date") ){
					if(maxOccurs > 1 || maxOccurs<0){
						joltObj.addProperty(name, "=toList");
					}else
					joltObj.addProperty(reader.getAttributeValue("","name"), "=toString");
				}else if(type.contains("tns:")){
					if(maxOccurs > 1 || maxOccurs<0){
						joltObj.addProperty(name.substring(0,name.length()-1)+"*", "=toList");
					}
					JsonObject obj = new JsonObject();
					joltObj.add(reader.getAttributeValue("","name"), obj);
					List<JsonObject> listObj= compMap.get(type.split(":")[1]);
					if(listObj==null){
						listObj = new ArrayList<JsonObject>();
					}
					listObj.add(obj);
					compMap.put(type.split(":")[1], listObj);
				}
				}else if(reader.getLocalName().equals("complexType")){
					for(JsonObject jObj:compMap.get(reader.getAttributeValue("","name")))
						nextElement(reader, jObj);
				}
			}
		}
		return spec.toString();
	}
	
	public static void nextElement(XMLStreamReader reader, JsonObject jobj) throws NumberFormatException, XMLStreamException, Exception {	
		while(reader.hasNext()){
			switch(reader.next()){
			case XMLEvent.START_ELEMENT:
				String type=reader.getAttributeValue("","type")==null?"":reader.getAttributeValue("","type");
				Integer maxOccurs = Integer.parseInt(reader.getAttributeValue("","maxOccurs")==null?"0":reader.getAttributeValue("","maxOccurs"));
				String name=reader.getAttributeValue("","name");
				
				if(type.contains("int")||type.contains("short")){
					if(maxOccurs > 1 || maxOccurs<0){
						jobj.addProperty(name, "=toList");
					}else
					jobj.addProperty(reader.getAttributeValue("","name"), "=toInteger");
				}else if(type.contains("decimal")){
					if(maxOccurs > 1 || maxOccurs<0){
						jobj.addProperty(name, "=toList");
					}else
					jobj.addProperty(reader.getAttributeValue("","name"), "=toDouble");
				}else if(type.contains("long")){
					if(maxOccurs > 1 || maxOccurs<0){
						jobj.addProperty(name, "=toList");
					}else
					jobj.addProperty(reader.getAttributeValue("","name"), "=toLong");
				}else if(type.contains("string")||type.contains("date") ){
					if(maxOccurs > 1 || maxOccurs<0){
						jobj.addProperty(name, "=toList");
					}else
					jobj.addProperty(reader.getAttributeValue("","name"), "=toString");
				}else if(type.contains("tns:")){
					if(maxOccurs > 1 || maxOccurs<0){
						jobj.addProperty(name.substring(0,name.length()-1)+"*", "=toList");
					}
					JsonObject obj = new JsonObject();
					jobj.add(reader.getAttributeValue("","name"), obj);
					List<JsonObject> listObj= compMap.get(type.split(":")[1]);
					if(listObj==null){
						listObj = new ArrayList<JsonObject>();
					}
					listObj.add(obj);
					compMap.put(type.split(":")[1], listObj);
				}else if(reader.getLocalName().equals("complexType")){
					nextElement(reader, compMap.get(reader.getAttributeValue("","name")));
				}
			}
		}
	}
		public static void nextElement(XMLStreamReader reader, List<JsonObject> objList) throws NumberFormatException, XMLStreamException{	
			while(reader.hasNext()){
				switch(reader.next()){
				case XMLEvent.START_ELEMENT:
					String type=reader.getAttributeValue("","type")==null?"":reader.getAttributeValue("","type");
					Integer maxOccurs = Integer.parseInt(reader.getAttributeValue("","maxOccurs")==null?"0":reader.getAttributeValue("","maxOccurs"));
					String name=reader.getAttributeValue("","name");
					
					if(type.contains("int")||type.contains("short")){
						for(JsonObject jobj:objList){
						if(maxOccurs > 1 || maxOccurs<0){
							jobj.addProperty(name, "=toList");
						}else
						jobj.addProperty(reader.getAttributeValue("","name"), "=toInteger");
						}
					}else if(type.contains("decimal")){
						for(JsonObject jobj:objList){
						if(maxOccurs > 1 || maxOccurs<0){
							jobj.addProperty(name, "=toList");
						}else
						jobj.addProperty(reader.getAttributeValue("","name"), "=toDouble");
					}
					}else if(type.contains("long")){
						for(JsonObject jobj:objList){
						if(maxOccurs > 1 || maxOccurs<0){
							jobj.addProperty(name, "=toList");
						}else
						jobj.addProperty(reader.getAttributeValue("","name"), "=toLong");
						}
					}else if(type.contains("string")||type.contains("date") ){
						for(JsonObject jobj:objList){
						if(maxOccurs > 1 || maxOccurs<0){
							jobj.addProperty(name, "=toList");
						}else
						jobj.addProperty(reader.getAttributeValue("","name"), "=toString");
						}
					}else if(type.contains("tns:")){
						for(JsonObject jobj:objList){
						if(maxOccurs > 1 || maxOccurs<0){
							jobj.addProperty(name.substring(0,name.length()-1)+"*", "=toList");
						}
						JsonObject obj = new JsonObject();
						jobj.add(reader.getAttributeValue("","name"), obj);
						List<JsonObject> listObj= compMap.get(type.split(":")[1]);
						if(listObj==null){
							listObj = new ArrayList<JsonObject>();
						}
						listObj.add(obj);
						compMap.put(type.split(":")[1], listObj);
						}
					}else if(reader.getLocalName().equals("complexType")){
						nextElement(reader,compMap.get(reader.getAttributeValue("","name")));
					}
				}
			}
		}


}
