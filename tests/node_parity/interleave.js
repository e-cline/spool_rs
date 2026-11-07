// Twin of `interleave` in tests/node_parity.rs.
for (const name of ["a", "b", "c"]) {
  queueMicrotask(async () => {
    for (let step = 1; step <= 3; step++) {
      console.log(`${name}${step}`);
      await null;
    }
  });
}
