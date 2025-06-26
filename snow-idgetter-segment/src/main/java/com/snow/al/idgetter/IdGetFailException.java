package com.snow.al.idgetter;

public class IdGetFailException extends RuntimeException{

    private static final long serialVersionUID = 8008782204947664945L;

    public IdGetFailException(String message) {
        super(message);
    }

    public IdGetFailException(String message, Throwable cause) {
        super(message, cause);
    }
}
