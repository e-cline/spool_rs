// Twin of `interval_with_timeouts` in tests/node_parity.rs.
async function main() {
  let n = 0;
  const h = setInterval(() => {
    n += 1;
    console.log(`i${n}`);
    if (n === 3) clearInterval(h);
  }, 20);
  setTimeout(() => console.log("t30"), 30);
  setTimeout(() => console.log("t50"), 50);
}
main();
