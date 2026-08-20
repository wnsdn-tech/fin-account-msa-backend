package com.finaccount.accountservice.security;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;

@Component
public class JwtProvider {

    private final SecretKey secretKey;
    private final long expirationInMillis;

    public JwtProvider(
            @Value("${token.secret}") String secret,
            @Value("${token.expiration-in-days}") long expirationInDays
    ) {
        this.secretKey = Keys.hmacShaKeyFor(
                secret.getBytes(StandardCharsets.UTF_8)
        );

        this.expirationInMillis =
                expirationInDays * 24 * 60 * 60 * 1000L;
    }

    public String generateToken(String accountNumber) {

        Date now = new Date();
        Date expiration = new Date(
                now.getTime() + expirationInMillis
        );

        return Jwts.builder()
                .subject(accountNumber)
                .issuedAt(now)
                .expiration(expiration)
                .signWith(secretKey)
                .compact();
    }
}