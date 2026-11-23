package org.raft.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestValueException;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;
import java.util.Map;

@RestControllerAdvice
public class ValidationExceptionHandler {

    @ExceptionHandler({MethodArgumentNotValidException.class,
                       MissingRequestValueException.class,
                       HandlerMethodValidationException.class})
    public ResponseEntity<Map<String, Object>> onValidation(Exception e) {
        List<Map<String, Object>> detail;
        if (e instanceof MethodArgumentNotValidException ex) {
            detail = ex.getFieldErrors().stream()
                    .map(fe -> Map.<String, Object>of(
                            "loc", List.of("body", fe.getField()),
                            "msg", fe.getDefaultMessage() == null ? "invalid value" : fe.getDefaultMessage(),
                            "type", "value_error"))
                    .toList();
        } else {
            detail = List.of(Map.of(
                    "loc", List.of("query"),
                    "msg", e.getMessage() == null ? "invalid value" : e.getMessage(),
                    "type", "value_error"));
        }
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(Map.of("detail", detail));
    }
}
