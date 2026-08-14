package com.finaccount.gateway.exception;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleException(
            Exception e,
            HttpServletRequest request
    ) {

        ErrorResponse response = new ErrorResponse(
                500,
                "Internal Server Error",
                request.getRequestURI()
        );

        return ResponseEntity
                .status(500)
                .body(response);
    }
}