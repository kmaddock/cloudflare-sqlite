import Module from "./sql-wasm.js";
import wasmModule from "./sql-wasm.wasm";

export default function initSQL() {
    async function instantiateWasm2(info, receiver) {
        let instance = await WebAssembly.instantiate(wasmModule, info);
        return receiver(instance);
    }
    return Module({"instantiateWasm": instantiateWasm2});
};
