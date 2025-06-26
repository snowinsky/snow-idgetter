package com.snow.al.idgetter;

public interface ISequenceRepository {
    /**
     * 获取当前表的最新当前最大值
     * @param bizTag
     * @return
     */
    Long getCurrentSequence(String bizTag);

    /**
     * 首先这个方法必须有独立的事务，独立提交，不可以和别的事务共享
     * 如果使用spring则设定propagation = Propagation.REQUIRES_NEW
     * 使用jdbc，必须手动commit。然后才可以确保有效。
     * @param bizTag
     * @param incrSize
     * @param currentSequence
     * @return
     */
    boolean increaseSequence(String bizTag, long incrSize, long currentSequence);
}
