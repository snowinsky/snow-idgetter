package com.snow.al.idgetter;

public interface IdGetterFactoryInterface {

    void register(String bizTag, long cachedSize);

    Long nextId(String bizTag);
}
