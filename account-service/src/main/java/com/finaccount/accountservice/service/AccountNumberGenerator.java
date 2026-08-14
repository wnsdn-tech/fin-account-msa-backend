package com.finaccount.accountservice.service;

import java.util.concurrent.ThreadLocalRandom;

public class AccountNumberGenerator {
    private static final String ACCOUNT_NUMBER_PREFIX = "110";

    public String generate() {
        return ACCOUNT_NUMBER_PREFIX + ThreadLocalRandom.current().nextLong(0L, 1_000_000_000L);
    }
}
