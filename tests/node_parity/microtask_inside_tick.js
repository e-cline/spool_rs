// Twin of `microtask_inside_tick` in tests/node_parity.rs.
async function main() {
  process.nextTick(() => {
    console.log("t1");
    queueMicrotask(() => console.log("m2 (queued by t1)"));
  });
  queueMicrotask(() => console.log("m1"));
}
main();
