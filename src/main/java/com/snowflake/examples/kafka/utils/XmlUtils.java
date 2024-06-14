package com.snowflake.examples.kafka.utils;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

public class XmlUtils {

    /**
     * Parses the provided XML string and validates its structure.
     * This method ensures that the XML data has a flat structure with no nested elements or attributes.
     * Each element must contain only text content and not contain any child elements or attributes.
     *
     * @param xmlData The XML string to be parsed.
     * @return A map containing the XML tag names as keys and their text content as values. The map also includes
     * a special key "XML$RAW_DOCUMENT" that contains the raw XML data, and "Metadata$Type" that contains
     * the root element's tag name. Returns null if the XML is invalid or does not meet the flat structure requirement.
     * @throws XmlParsingException if an error occurs during XML parsing. This includes issues like malformed XML data.
     */
    public static Map<String, String> parseAndValidateXML(String xmlData) throws XmlParsingException {
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(new InputSource(new StringReader(xmlData)));
            doc.getDocumentElement().normalize();

            Element root = doc.getDocumentElement();
            NodeList nodes = root.getChildNodes();
            Map<String, String> data = new HashMap<>();

            for (int i = 0; i < nodes.getLength(); i++) {
                if (nodes.item(i) instanceof Element) {
                    Element element = (Element) nodes.item(i);
                    if (element.hasAttributes() || element.getChildNodes().getLength() > 1) {
                        //TODO add more logic to handle this case (if required)
                        throw new IllegalArgumentException("XML contains unsupported attributes or deeply nested data");
                    }
                    data.put(element.getTagName(), element.getTextContent());
                }
            }
            data.put("XML$TYPE", root.getTagName());
            data.put("XML$RAW_DOCUMENT", xmlData);
            return data;
        } catch (Exception e) {
            throw new XmlParsingException("Unable to parse and validate XML", e);
        }
    }
}
