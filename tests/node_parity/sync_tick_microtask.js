// Twin of `sync_tick_microtask` in tests/node_parity.rs.
async function main() {
  queueMicrotask(() => console.log("microtask"));
  process.nextTick(() => console.log("tick"));
  console.log("sync");
}
main();
