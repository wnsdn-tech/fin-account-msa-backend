package com.finaccount.accountservice.vo;

import com.finaccount.accountservice.dto.AccountStatus;
import lombok.Data;

@Data
public class AccountResponse {
    private String accountNumber;

    private String ownerName;

    private Long balance;

    private AccountStatus status;
}
