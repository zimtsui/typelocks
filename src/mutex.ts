import { Semaphore } from './semaphore.ts';


/**
 * A mutex is initially acquired.
 */
export class Mutex<T> implements AsyncIterableIterator<T, never, void> {

    protected sem = new Semaphore<T>();

    public isAcquired(): boolean {
        return !this.sem.getSize();
    }

    public acquire(signal?: AbortSignal): Promise<T> {
        return this.sem.decrease(signal);
    }

    /**
     * @throws {@link RangeError}
     */
    public acquireSync(): T {
        return this.sem.decreaseSync();
    }

    /**
     * @throws {@link RangeError}
     */
    public release(x: T): void {
        if (this.isAcquired()) {} else throw new RangeError();
        this.sem.increase(x);
    }

    public releaseTry(x: T): void {
        if (this.isAcquired()) this.release(x);
    }

    public abort(e: unknown): void {
        return this.sem.abort(e);
    }

    public next(): Promise<IteratorYieldResult<T>> {
        return this.sem.next();
    }

    public [Symbol.asyncIterator]() {
        return this;
    }
}

export namespace Mutex {

    /**
     * A void mutex is initially released.
     */
    export class Void extends Mutex<void> {
        public constructor() {
            super();
            this.release();
        }

        public async acquireRaii(signal?: AbortSignal): Promise<Disposable> {
            await this.acquire(signal);
            return {
                [Symbol.dispose]: (): void => {
                    this.release();
                },
            };
        }
    }
}
