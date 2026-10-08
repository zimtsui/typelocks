

export class FiniteSemaphore<T> implements AsyncIterableIterator<T, never, void> {
    protected consumers: [PromiseWithResolvers<T>, AbortSignal][] = [];
    protected producers: [PromiseWithResolvers<void>, AbortSignal, T][] = [];
    protected products: T[] = [];

    public constructor(protected capacity: number) {
        if (Number.isSafeInteger(this.capacity) && this.capacity >= 0) {} else throw new Error();
    }

    public getSize(): number {
        return this.products.length;
    }

    protected flush(): void {
        this.consumers = this.consumers.filter(([pwr, signal]) => !signal.aborted);
        this.producers = this.producers.filter(([pwr, signal]) => !signal.aborted);
        while (this.products.length && this.consumers.length) {
            const [pwr] = this.consumers.shift()!;
            pwr.resolve(this.products.shift()!);
        }
        while (this.consumers.length && this.producers.length) {
            const [pwrProducer, signalProducer, x] = this.producers.shift()!;
            const [pwrConsumer] = this.consumers.shift()!;
            pwrProducer.resolve();
            pwrConsumer.resolve(x);
        }
        while (this.products.length < this.capacity && this.producers.length) {
            const [pwr, signal, x] = this.producers.shift()!;
            this.products.push(x);
            pwr.resolve();
        }
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
        if (this.products.length > 0) {
            const x = this.products.shift()!;
            this.flush();
            return x;
        } else if (this.producers.length) {
            const [pwr, signal, x] = this.producers.shift()!;
            pwr.resolve();
            return x;
        } else throw new RangeError();
    }

    public async increase(x: T, signal?: AbortSignal): Promise<void> {
        signal?.throwIfAborted();
        const ac = new AbortController();
        const pwr = Promise.withResolvers<void>();
        signal?.addEventListener('abort', () => pwr.reject(signal.reason), { signal: ac.signal });
        this.producers.push([pwr, signal ?? new AbortController().signal, x]);
        this.flush();
        await pwr.promise.finally(() => ac.abort());
    }

    /**
     * @throws {@link RangeError}
     */
    public increaseSync(x: T): void {
        this.flush();
        if (this.products.length < this.capacity) {
            this.products.push(x);
            this.flush();
        } else if (this.consumers.length) {
            const [pwr, signal] = this.consumers.shift()!;
            pwr.resolve(x);
        } else throw new RangeError();
    }

    public async next(): Promise<IteratorYieldResult<T>> {
        return {
            done: false,
            value: await this.decrease(),
        };
    }

    public abort(e: unknown): void {
        for (const [pwr] of this.consumers) pwr.reject(e);
        for (const [pwr] of this.producers) pwr.reject(e);
        this.consumers = [];
        this.producers = [];
        this.products = this.products.slice(0, this.capacity);
    }

    public [Symbol.asyncIterator]() {
        return this;
    }
}

export namespace FiniteSemaphore {

    export class Void extends FiniteSemaphore<void> {
        public async decreaseRaii(signal?: AbortSignal): Promise<AsyncDisposable> {
            await this.decrease(signal);
            return {
                [Symbol.asyncDispose]: async (): Promise<void> => {
                    await this.increase();
                },
            };
        }

        public async increaseRaii(signal?: AbortSignal): Promise<AsyncDisposable> {
            await this.increase(undefined, signal);
            return {
                [Symbol.asyncDispose]: async (): Promise<void> => {
                    await this.decrease();
                },
            };
        }
    }

}
