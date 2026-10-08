import { Mutex } from './mutex.ts';


export class ConditionVariable {
	protected listeners: [PromiseWithResolvers<void>, AbortSignal][] = [];
	public mutex = new Mutex.Void();

	public async wait(signal?: AbortSignal): Promise<void> {
		signal?.throwIfAborted();
		const ac = new AbortController();
		this.mutex.release();
		const pwr = Promise.withResolvers<void>();
		signal?.addEventListener('abort', () => pwr.reject(signal.reason), { signal: ac.signal });
		this.listeners.push([pwr, signal ?? new AbortController().signal]);
		try {
			await pwr.promise;
			await this.mutex.acquire(signal);
		} finally {
			ac.abort();
		}
	}

	public signal(): void {
		this.listeners = this.listeners.filter(([pwr, signal]) => !signal.aborted);
		if (this.listeners.length) {
			const [pwr] = this.listeners.shift()!;
			pwr.resolve();
		}
	}

	public broadcast(): void {
		for (const [pwr] of this.listeners) pwr.resolve();
		this.listeners = [];
	}

	public abort(e: unknown): void {
		this.mutex.abort(e);
		for (const [pwr] of this.listeners) pwr.reject(e);
		this.listeners = [];
	}
}
