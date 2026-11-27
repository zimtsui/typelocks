

export abstract class RWLockBase {
    protected readers: [PromiseWithResolvers<void>, AbortSignal][] = [];
    protected writers: [PromiseWithResolvers<void>, AbortSignal][] = [];
    protected reading = 0;
    protected writing = false;

    public isAcquiredRead(): boolean {
        this.flush();
        return !!this.reading;
    }

    public isAcquiredWrite(): boolean {
        this.flush();
        return this.writing;
    }

    public async acquireRead(signal?: AbortSignal): Promise<void> {
        signal?.throwIfAborted();
        using dk = new DisposableStack();
        const pwr = Promise.withResolvers<void>();
        signal?.addEventListener('abort', dk.adopt(
            () => pwr.reject(signal.reason),
            listener => signal.removeEventListener('abort', listener),
        ));
        this.readers.push([pwr, signal ?? new AbortController().signal]);
        this.flush();
        await pwr.promise;
    }

    /**
     * @throws {@link RangeError}
     */
    public abstract acquireReadSync(): void;

    public abstract acquireReadTry(): void;

    public async acquireWrite(signal?: AbortSignal): Promise<void> {
        signal?.throwIfAborted();
        using dk = new DisposableStack();
        const pwr = Promise.withResolvers<void>();
        signal?.addEventListener('abort', dk.adopt(
            () => {
                pwr.reject(signal.reason);
                this.flush();
            },
            listener => signal.removeEventListener('abort', listener),
        ));
        this.writers.push([pwr, signal ?? new AbortController().signal]);
        this.flush();
        await pwr.promise;
    }

    /**
     * @throws {@link RangeError}
     */
    public acquireWriteSync(): void {
        this.flush();
        if (!this.writing && !this.reading) {} else throw new RangeError();
        this.writing = true;
    }

    public acquireWriteTry(): void {
        this.flush();
        if (!this.writing && !this.reading) this.writing = true;
    }

    /**
     * @throws {@link RangeError}
     */
    public releaseRead(): void {
        if (this.reading) {} else throw new RangeError();
        this.reading--;
        this.flush();
    }

    public releaseReadTry(): void {
        this.flush();
        if (this.reading) {
            this.reading--;
            this.flush();
        }
    }

    /**
     * @throws {@link RangeError}
     */
    public releaseWrite(): void {
        if (this.writing) {} else throw new RangeError();
        this.writing = false;
        this.flush();
    }

    public releaseWriteTry(): void {
        this.flush();
        if (this.writing) {
            this.writing = false;
            this.flush();
        }
    }

    public abort(e: unknown): void {
        for (const [pwr] of this.readers) pwr.reject(e);
        for (const [pwr] of this.writers) pwr.reject(e);
        this.readers = [];
        this.writers = [];
    }

    /**
     * @throws {@link RangeError}
     */
    public switch(): void {
        if (this.writing) {} else throw new RangeError();
        this.writing = false;
        this.reading = 1;
        this.flush();
    }

    protected abstract flush(): void;

    public async acquireReadRaii(signal?: AbortSignal): Promise<Disposable> {
        await this.acquireRead(signal);
        return {
            [Symbol.dispose]: (): void => {
                this.releaseRead();
            },
        };
    }

    /**
     * @throws {@link RangeError}
     */
    public acquireReadRaiiSync(): Disposable {
        this.acquireReadSync();
        return {
            [Symbol.dispose]: (): void => {
                this.releaseRead();
            },
        };
    }

    public async acquireWriteRaii(signal?: AbortSignal): Promise<Disposable> {
        await this.acquireWrite(signal);
        return {
            [Symbol.dispose]: (): void => {
                this.releaseWrite();
            },
        };
    }

    /**
     * @throws {@link RangeError}
     */
    public acquireWriteRaiiSync(): Disposable {
        this.acquireWriteSync();
        return {
            [Symbol.dispose]: (): void => {
                this.releaseWrite();
            },
        };
    }
}
