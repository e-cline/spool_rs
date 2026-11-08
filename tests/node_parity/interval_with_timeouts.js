// Twin of `interval_with_timeouts` in tests/node_parity.rs.
async function main() {
  let n = 0;
  const h = setInterval(() => {
    n += 1;
    console.log(`i${n}`);
    if (n === 3) clearInterval(h);
  }, 100);
  setTimeout(() => console.log("t150"), 150);
  setTimeout(() => console.log("t260"), 260);
}
main();
