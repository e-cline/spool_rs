// Twin of `spawn_after_yield` in tests/node_parity.rs.
queueMicrotask(async () => {
  console.log("a1");
  await null;
  console.log("a2");
  queueMicrotask(async () => console.log("a-child"));
  console.log("a3");
});
queueMicrotask(async () => {
  console.log("b1");
  await null;
  console.log("b2");
  await null;
  console.log("b3");
});
