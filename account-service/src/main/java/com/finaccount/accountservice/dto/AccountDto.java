package com.finaccount.accountservice.dto;

import lombok.Data;

@Data
public class AccountDto {
    private String accountNumber;

    private String ownerName;

    private String password;

    private Long balance;

    private AccountStatus status;
}
