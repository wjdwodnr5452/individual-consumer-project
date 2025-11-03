package com.example.individual_consumer_project.comm.encrypt;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EncryptionService {

    private final Encrypt encrypt;

    @Value("${encryption.key}")
    private String encryptionKey;


    public String encryptSha256(String value) {
        String encrypted = encrypt.encryptSha256(value);
        return encrypted;
    }

    public String encryptAes(String str) {

        try {
            String encrypted = encrypt.encryptAes(str, encryptionKey);
            return encrypted;
        } catch (Exception e) {
            throw new RuntimeException("encryption exception", e);
        }

    }

    public String decryptAes(String str) {

        try {
            String decryptString = encrypt.decryptAes(str, encryptionKey);
            return decryptString;
        } catch (Exception e) {
            throw new RuntimeException("decryption exception", e);
        }

    }

}
