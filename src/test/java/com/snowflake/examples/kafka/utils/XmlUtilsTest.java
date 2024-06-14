package com.snowflake.examples.kafka.utils;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

public class XmlUtilsTest {

    @Test
    public void testParseAndValidateXML() throws XmlParsingException {
        String xmlInput = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<LiftTicket>\n" +
                "    <TicketID>12345</TicketID>\n" +
                "    <ResortName>Alpine Peaks</ResortName>\n" +
                "    <TicketType>Day Pass</TicketType>\n" +
                "    <DateIssued>2024-06-11</DateIssued>\n" +
                "    <ValidFrom>2024-06-11</ValidFrom>\n" +
                "    <ValidTo>2024-06-11</ValidTo>\n" +
                "    <Price>120.00</Price>\n" +
                "    <Currency>USD</Currency>\n" +
                "    <PurchasedBy>John Doe</PurchasedBy>\n" +
                "    <AgeGroup>Adult</AgeGroup>\n" +
                "    <AccessLevel>All Mountains</AccessLevel>\n" +
                "</LiftTicket>";

        Map<String, String> parsedOutput = XmlUtils.parseAndValidateXML(xmlInput);

        assertNotNull(parsedOutput, "Parsed output should not be null.");

        assertEquals("12345", parsedOutput.get("TicketID"));
        assertEquals("Alpine Peaks", parsedOutput.get("ResortName"));
        assertEquals("Day Pass", parsedOutput.get("TicketType"));
        assertEquals("2024-06-11", parsedOutput.get("DateIssued"));
        assertEquals("120.00", parsedOutput.get("Price"));
        assertEquals("USD", parsedOutput.get("Currency"));
        assertEquals("John Doe", parsedOutput.get("PurchasedBy"));
        assertEquals("Adult", parsedOutput.get("AgeGroup"));
        assertEquals("All Mountains", parsedOutput.get("AccessLevel"));

        assertEquals(xmlInput, parsedOutput.get("XML$RAW_DOCUMENT"), "Payload in XML$RAW_DOCUMENT field should match XML input");

        assertEquals("LiftTicket", parsedOutput.get("XML$TYPE"), "Metadata type should match the root element name.");
    }
}
