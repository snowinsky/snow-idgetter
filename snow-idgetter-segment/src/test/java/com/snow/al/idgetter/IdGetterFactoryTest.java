package com.snow.al.idgetter;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

@Slf4j
public class IdGetterFactoryTest {

    @Test
    public void register() {
        Map<String, Long> table = new HashMap<>();
        ISequenceRepository a = new ISequenceRepository() {
            @Override
            public Long getCurrentSequence(String bizTag) {
                table.putIfAbsent(bizTag, 0L);
                return table.getOrDefault(bizTag, 0L);
            }

            @Override
            public boolean increaseSequence(String bizTag, long incrSize, long currentSequence) {
                table.computeIfPresent(bizTag, new BiFunction<String, Long, Long>() {
                    @Override
                    public Long apply(String s, Long aLong) {
                        if (aLong == currentSequence) {
                            return currentSequence + incrSize;
                        }
                        return aLong;
                    }
                });
                return table.get(bizTag) == currentSequence + incrSize;
            }
        };

        for (int i = 0; i < 5; i++) {
            Long old = a.getCurrentSequence("t_table1");
            log.info("old = {}", old);
            boolean update = a.increaseSequence("t_table1", 4, old);
            log.info("update ={}, table={}", update, table);
        }

    }

    @Test
    public void nextId() {

        Map<String, Long> table = new HashMap<>();
        IdGetterFactory factory = new IdGetterFactory(
                new ISequenceRepository() {

                    @Override
                    public Long getCurrentSequence(String bizTag) {
                        table.putIfAbsent(bizTag, 0L);
                        return table.getOrDefault(bizTag, 0L);
                    }

                    @Override
                    public boolean increaseSequence(String bizTag, long incrSize, long currentSequence) {
                        table.computeIfPresent(bizTag, new BiFunction<String, Long, Long>() {
                            @Override
                            public Long apply(String s, Long aLong) {
                                if (aLong == currentSequence) {
                                    return currentSequence + incrSize;
                                }
                                return aLong;
                            }
                        });
                        return table.get(bizTag) == currentSequence + incrSize;
                    }
                });
        factory.register("t_table1", 6L);
        factory.register("t_table2", 8L);
        factory.register("t_table3", 4L);

        for (int i = 0; i < 18; i++) {
            log.info("i={} before, table={}", i, table);
            log.info("table1 getNextId={}", factory.nextId("t_table1"));
            log.info("table2 getNextId={}", factory.nextId("t_table2"));
            log.info("table3 getNextId={}", factory.nextId("t_table3"));
            log.info("i={} after, table={}", i, table);
        }
    }
}