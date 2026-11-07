// Twin of `nested_spawn` in tests/node_parity.rs.
queueMicrotask(async () => {
  console.log("outer start");
  queueMicrotask(async () => {
    console.log("inner start");
    await null;
    console.log("inner end");
  });
  await null;
  console.log("outer end");
});
queueMicrotask(async () => {
  console.log("sibling start");
  await null;
  console.log("sibling end");
});
