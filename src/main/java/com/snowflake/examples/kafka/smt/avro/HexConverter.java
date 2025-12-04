package com.snowflake.examples.kafka.smt.avro;

import org.apache.commons.codec.binary.Hex;

/**
 * Hex encoding utility using Apache Commons Codec.
 * 
 * Delegates to the well-tested org.apache.commons.codec.binary.Hex class
 * for reliable, efficient hex encoding.
 */
class HexConverter {

    private final String prefix;
    private final boolean uppercase;

    public HexConverter(String prefix, boolean uppercase) {
        this.prefix = prefix;
        this.uppercase = uppercase;
    }

    /**
     * Convert byte array to hex string using Apache Commons Codec.
     * 
     * @param bytes the byte array to encode
     * @return hex-encoded string with optional prefix, or null if input is null
     */
    public String toHex(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        // Use Apache Commons Codec for hex encoding
        char[] hexChars = Hex.encodeHex(bytes, !uppercase);
        
        // Add prefix if configured
        if (prefix.isEmpty()) {
            return new String(hexChars);
        } else {
            return prefix + new String(hexChars);
        }
    }
}

