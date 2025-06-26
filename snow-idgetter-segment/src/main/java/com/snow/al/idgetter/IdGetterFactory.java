package com.snow.al.idgetter;

import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Setter
@RequiredArgsConstructor
public class IdGetterFactory implements IdGetterFactoryInterface {

    private static final ExecutorService THREAD_POOL = new ThreadPoolExecutor(5, 20,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(16), new ThreadFactory() {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("ID-GETTER-THREAD-" + threadNumber.getAndIncrement());
            return t;
        }
    }, new ThreadPoolExecutor.CallerRunsPolicy());


    private static final ConcurrentHashMap<String, IdGetter> bizTagIdLeaf = new ConcurrentHashMap<>(160);
    private static final ConcurrentHashMap<String, Long> bizTagIdCacheSize = new ConcurrentHashMap<>(160);

    private final ISequenceRepository sequenceRepository;

    private Long getIdByTableName(String tableName) {
        if (bizTagIdLeaf.get(tableName) == null) {
            synchronized (bizTagIdLeaf) {
                if (bizTagIdLeaf.get(tableName) == null) {
                    IdGetter idGetter = new IdGetter();
                    idGetter.setBizTag(tableName);
                    idGetter.setAsyncLoadingSegment(true);
                    idGetter.setTaskExecutor(THREAD_POOL);
                    idGetter.setSequenceRepository(sequenceRepository);
                    idGetter.setIncrSize(bizTagIdCacheSize.getOrDefault(tableName, 50L));
                    idGetter.init();
                    bizTagIdLeaf.putIfAbsent(tableName, idGetter);
                }
            }
        }
        return bizTagIdLeaf.get(tableName).getId();
    }

    @Override
    public void register(String bizTag, long range) {
        if (bizTagIdCacheSize.get(bizTag) == null) {
            synchronized (bizTagIdCacheSize) {
                bizTagIdCacheSize.putIfAbsent(bizTag, range);
            }
        }
    }

    @Override
    public Long nextId(String bizTag) {
        return getIdByTableName(bizTag);
    }

}
