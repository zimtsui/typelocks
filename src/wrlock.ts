import { RWLockBase } from './rwlock-base.ts';


/**
 * Write priority
 */
export class WRLock extends RWLockBase {
    protected flush(): void {
        this.readers = this.readers.filter(([pwr, signal]) => !signal.aborted);
        this.writers = this.writers.filter(([pwr, signal]) => !signal.aborted);
        if (!this.writing && !this.writers.length && this.readers.length) {
            this.reading += this.readers.length;
            for (const [pwr] of this.readers) pwr.resolve();
            this.readers = [];
        } else if (!this.reading && !this.writing && this.writers.length) {
            this.writing = true;
            const [pwr] = this.writers.shift()!;
            pwr.resolve();
        }
    }

    public override acquireReadSync(): void {
        this.flush();
        if (!this.writing && !this.writers.length) {} else throw new RangeError();
        this.reading++;
    }

    public override acquireReadTry(): void {
        this.flush();
        if (!this.writing && !this.writers.length) this.reading++;
    }
}
