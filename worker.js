import init, { __worker_drain, __notify_index } from "/frontend_wasm.js";

self.onmessage = async ({ data }) => {
  const [module, memory, workerId] = data;
  const wasm = await init({ module_or_path: module, memory });

  const idx = __notify_index(workerId);

  while (true) {
    const view = new Int32Array(wasm.memory.buffer);
    const seen = Atomics.load(view, idx);

    if (!__worker_drain(workerId)) break; // false => shutting down

    const r = Atomics.waitAsync(view, idx, seen);
    if (r.async) {
      await r.value;
    }
  }

  console.log(`[worker ${workerId}] is going offline!`);
};
