// Twin of `tick_inside_tick` in tests/node_parity.rs.
async function main() {
  process.nextTick(() => {
    console.log("t1");
    process.nextTick(() => console.log("t3 (queued by t1)"));
  });
  process.nextTick(() => console.log("t2"));
  queueMicrotask(() => console.log("m"));
}
main();
