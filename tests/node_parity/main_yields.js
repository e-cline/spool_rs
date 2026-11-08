// Twin of `main_yields` in tests/node_parity.rs.
async function main() {
  queueMicrotask(() => console.log("m1"));
  console.log("main before yield");
  await null;
  console.log("main after yield");
  queueMicrotask(() => console.log("m2"));
}
main();
