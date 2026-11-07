// Twin of `yield_microtask_interleave` in tests/node_parity.rs.
async function main() {
  queueMicrotask(() => console.log("A"));
  queueMicrotask(async () => {
    console.log("B");
    await null;
    console.log("D");
  });
  queueMicrotask(() => console.log("C"));
}
main();
