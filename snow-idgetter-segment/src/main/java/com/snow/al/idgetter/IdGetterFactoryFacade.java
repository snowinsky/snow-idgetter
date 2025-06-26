package com.snow.al.idgetter;

public interface IdGetterFactoryFacade {

    void register(String bizTag, long cachedSize);

    Long nextId(String bizTag);
}
