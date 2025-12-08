package com.snowflake.examples.kafka.smt.avro;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

/**
 * Byte encoding utility supporting hex and base64 encoding.
 * 
 * Delegates to Apache Commons Codec for reliable, efficient encoding.
 */
class ByteEncoder {

    public enum Encoding {
        HEX,
        BASE64
    }

    private final Encoding encoding;
    private final String prefix;
    private final boolean uppercase;

    public ByteEncoder(Encoding encoding, String prefix, boolean uppercase) {
        this.encoding = encoding;
        this.prefix = prefix;
        this.uppercase = uppercase;
    }

    /**
     * Convert byte array to encoded string using Apache Commons Codec.
     * 
     * @param bytes the byte array to encode
     * @return encoded string with optional prefix, or null if input is null
     */
    public String encode(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        String encoded;
        switch (encoding) {
            case HEX:
                char[] hexChars = Hex.encodeHex(bytes, !uppercase);
                encoded = new String(hexChars);
                break;
            case BASE64:
                encoded = Base64.encodeBase64String(bytes);
                break;
            default:
                throw new IllegalStateException("Unsupported encoding: " + encoding);
        }
        
        // Add prefix if configured
        if (prefix.isEmpty()) {
            return encoded;
        } else {
            return prefix + encoded;
        }
    }
}

