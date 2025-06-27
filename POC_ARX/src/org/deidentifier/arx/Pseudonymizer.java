package org.deidentifier.arx;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

public class Pseudonymizer {
    private final Map<String, String> map = new HashMap<>();
    private final SecureRandom random = new SecureRandom();
    private final int length;
    private final String charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    public Pseudonymizer(int length) {
        this.length = length;
    }

    private String generateRandomId() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(charset.charAt(random.nextInt(charset.length())));
        }
        return sb.toString();
    }

    public String get(String id) {
        return map.computeIfAbsent(id, k -> generateRandomId());
    }
}
