package com.finaccount.accountservice.controller;

import com.finaccount.accountservice.vo.AccountRequest;
import com.finaccount.accountservice.dto.AccountDto;
import com.finaccount.accountservice.service.AccountService;
import com.finaccount.accountservice.vo.AccountResponse;
import org.modelmapper.ModelMapper;
import org.modelmapper.convention.MatchingStrategies;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.NoSuchElementException;

@RestController
public class AccountController {
    private final AccountService service;

    public AccountController(AccountService service) {
        this.service = service;
    }

    @PostMapping("/accounts")
    public ResponseEntity<AccountResponse> createAccount(@RequestBody AccountRequest request) {
        ModelMapper mapper = new ModelMapper();
        mapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);

        AccountDto dto = service.createAccount(mapper.map(request, AccountDto.class));

        AccountResponse response = mapper.map(dto, AccountResponse.class);

        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/accounts/{accountId}")
    public ResponseEntity<AccountResponse> getAccount(@PathVariable Integer accountId) {
        try {
            ModelMapper mapper = new ModelMapper();
            mapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);

            AccountDto dto = service.getAccount(accountId);

            AccountResponse response = mapper.map(dto, AccountResponse.class);

            return ResponseEntity.status(HttpStatus.OK).body(response);

        } catch (NoSuchElementException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
    }
}