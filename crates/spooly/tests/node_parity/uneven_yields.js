// Twin of `uneven_yields` in tests/node_parity.rs.
for (const [name, yields] of [["a", 1], ["b", 3], ["c", 0]]) {
  queueMicrotask(async () => {
    console.log(`${name} start`);
    for (let i = 0; i < yields; i++) {
      await null;
      console.log(`${name} y${i}`);
    }
    console.log(`${name} end`);
  });
}
