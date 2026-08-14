package com.finaccount.accountservice.jpa;

import com.finaccount.accountservice.dto.AccountStatus;
import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
@Table(name = "accounts")
public class AccountEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer accountId;

    @Column(nullable = false, unique = true, length = 20)
    private String accountNumber;

    @Column(nullable = false, length = 100)
    private String ownerName;

    @Column(nullable = false, length = 100)
    private String password;

    @Column(nullable = false, length = 100)
    private Long balance;

    @Column(nullable = false, length = 100)
    @Enumerated(EnumType.ORDINAL)
    private AccountStatus status;
}
