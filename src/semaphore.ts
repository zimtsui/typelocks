

export class Semaphore<T> implements AsyncIterableIterator<T, never, void> {
    protected consumers: [PromiseWithResolvers<T>, AbortSignal][] = [];
    protected products: T[] = [];

    protected flush(): void {
        this.consumers = this.consumers.filter(([pwr, signal]) => !signal.aborted);
        while (this.products.length && this.consumers.length) {
            const [pwr] = this.consumers.shift()!;
            pwr.resolve(this.products.shift()!);
        }
    }

    public getSize(): number {
        return this.products.length;
    }

    public async decrease(signal?: AbortSignal): Promise<T> {
        signal?.throwIfAborted();
        const ac = new AbortController();
        const pwr = Promise.withResolvers<T>();
        signal?.addEventListener('abort', () => pwr.reject(signal.reason), { signal: ac.signal });
        this.consumers.push([pwr, signal ?? new AbortController().signal]);
        this.flush();
        return await pwr.promise.finally(() => ac.abort());
    }

    /**
     * @throws {@link RangeError}
     */
    public decreaseSync(): T {
        this.flush();
        if (this.products.length) {} else throw new RangeError();
        return this.products.shift()!;
    }

    public increase(x: T): void {
        this.products.push(x);
        this.flush();
    }

    public async next(): Promise<IteratorYieldResult<T>> {
        return {
            done: false,
            value: await this.decrease(),
        };
    }

    public abort(e: unknown): void {
        for (const [pwr] of this.consumers) pwr.reject(e);
        this.consumers = [];
    }

    public [Symbol.asyncIterator]() {
        return this;
    }
}

export namespace Semaphore {

    export class Void extends Semaphore<void> {
        public async decreaseRaii(): Promise<Disposable> {
            await this.decrease();
            return {
                [Symbol.dispose]: (): void => {
                    this.increase();
                },
            };
        }
    }
}
