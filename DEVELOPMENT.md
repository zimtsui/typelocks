# 非原子锁

用两个原子互斥锁实现的读写锁，加解锁过程没有原子性。

加锁过程中，获得第一把锁之后，获得第二把锁之前，可能会切到另一个线程。你不知道第二把锁将会被读者还是写者抢到，因此此刻锁的状态既不算 RDLOCKED，也不算 WRLOCKED。且此时 tryrdlock 和 trywrlock 都会失败，因此也不算 UNLOCKED 状态。此时这个锁处于三个状态之外的中间状态。

如果协程锁这么实现的话，unlock 就得写成异步函数。

# 同步的 state getter 函数

同步的 state getter 有意义，仅当

-   state getter 不会在不一致的中间状态被调用。
-   state getter 返回时例程不会切出去。

线程锁的 state getter 返回时可能切到别的线程去，所以不能调用 state getter。用互斥锁实现的协程读写锁，state getter 可能在中间状态被调用，所以也不能调用 state getter。只有原生实现的协程锁，才能调用 state getter。
