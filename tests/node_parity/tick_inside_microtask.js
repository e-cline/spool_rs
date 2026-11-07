// Twin of `tick_inside_microtask` in tests/node_parity.rs.
async function main() {
  queueMicrotask(() => {
    console.log("m1");
    process.nextTick(() => console.log("t2 (queued by m1)"));
  });
  queueMicrotask(() => console.log("m2"));
  process.nextTick(() => console.log("t1"));
}
main();
