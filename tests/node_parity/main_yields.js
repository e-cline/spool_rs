// Twin of `main_yields` in tests/node_parity.rs.
async function main() {
  process.nextTick(() => console.log("t1"));
  queueMicrotask(() => console.log("m1"));
  console.log("main before yield");
  await null;
  console.log("main after yield");
  process.nextTick(() => console.log("t2"));
  queueMicrotask(() => console.log("m2"));
}
main();
