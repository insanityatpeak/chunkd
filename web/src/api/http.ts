import type { ClusterAPI, ClusterState } from './cluster';

// HttpClusterAPI will poll a real gateway's state endpoint once the gateway
// exposes one. Until then it fails loudly instead of showing fake data.
export class HttpClusterAPI implements ClusterAPI {
  constructor(private readonly baseUrl: string) {}

  start(_seed: number): Promise<void> {
    return Promise.reject(new Error(`HttpClusterAPI(${this.baseUrl}) is not implemented yet`));
  }

  subscribe(_fn: (s: ClusterState) => void): () => void {
    return () => {};
  }

  pause() {}
  resume() {}
  setSpeed(_simMsPerSec: number) {}
  dispose() {}
}
